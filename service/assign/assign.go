package assign

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cestlascorpion/sardine/utils"
	"github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"
)

// Assign watch /segment/{table}/alloc/{addr} and /segment/{table}/section/{tag}/{hashId}.
// assign 'pending' section to a certain alloc and put routing rule(with 'pending' value). delete --prefix
// routing key in case alloc crashed. watch routing rule to manage idle section to re-assign.
type Assign struct {
	table        string
	name         string
	client       *v3.Client
	alloc        *allocTable
	watchCancel  []context.CancelFunc
	assignRetry  chan *reAssignInfo
	electionTTL  int
	master       *atomic.Bool
	checkPending string
	checkBalance string
	cronjob      *cron.Cron
	snapshots    []map[string][]string
	msgBot       *utils.LarkBot
}

func NewAssign(ctx context.Context, conf *utils.Config) (*Assign, error) {
	bot, err := utils.NewLarkBot(ctx, conf)
	if err != nil {
		log.Errorf("new lark bot err %+v", err)
		return nil, err
	}

	endpoints := strings.Split(conf.Storage.Etcd.Endpoints, ",")
	cli, err := v3.New(v3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		log.Errorf("v3 new err %+v", err)
		return nil, err
	}

	assign := &Assign{
		table:  conf.GetTable(),
		name:   conf.GetEtcdId(),
		client: cli,
		alloc: &allocTable{
			table: make(map[string]*status),
		},
		assignRetry:  make(chan *reAssignInfo, 1024),
		electionTTL:  conf.Storage.Etcd.ElectionTTL,
		master:       atomic.NewBool(false),
		checkPending: conf.Cronjob.CheckPending,
		checkBalance: conf.Cronjob.CheckBalance,
		cronjob:      cron.New(),
		snapshots:    make([]map[string][]string, 3),
		msgBot:       bot,
	}

	assign.watchAlloc(ctx)

	prefix := fmt.Sprintf(allocPrefixFormat, assign.table)
	resp, err := cli.Get(ctx, prefix, v3.WithPrefix())
	if err != nil {
		log.Errorf("etcd get err %+v", err)
		return nil, err
	}

	for i := range resp.Kvs {
		assign.alloc.putAlloc(ctx, resp.Kvs[i].Key, resp.Kvs[i].Value, resp.Kvs[i].ModRevision)
	}

	assign.doRetry(ctx)
	assign.watchSect(ctx)
	assign.watchRule(ctx)

	assign.election(ctx)
	err = assign.runCronjob(ctx)
	if err != nil {
		log.Errorf("assign run cronjob err %+v", err)
		return nil, err
	}

	log.Infof("assign %s ready to go", assign.name)
	assign.msgBot.SendMsg(ctx, "assign %s: ready to go", assign.name)
	return assign, nil
}

func (a *Assign) RegSection(ctx context.Context, tag string, async bool) error {
	if async {
		go func() {
			err := a.regSection(context.Background(), tag)
			if err != nil {
				log.Errorf("RegSection %s err %+v", tag, err)
				return
			}
		}()
		return nil
	}
	return a.regSection(ctx, tag)
}

func (a *Assign) UnRegSection(ctx context.Context, tag string, async bool) error {
	if async {
		go func() {
			err := a.unRegSection(context.Background(), tag)
			if err != nil {
				log.Errorf("UnRegSection %s err %+v", tag, err)
				return
			}
		}()
		return nil
	}
	return a.unRegSection(ctx, tag)
}

func (a *Assign) Close(ctx context.Context) error {
	a.cronjob.Stop()
	for i := range a.watchCancel {
		a.watchCancel[i]()
	}
	log.Infof("cancel watch go routine")
	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

const (
	allocPrefixFormat   = "/segment/%s/alloc"
	sectionPrefixFormat = "/segment/%s/section"
	routingPrefixFormat = "/segment/%s/routing"
	assignMasterFormat  = "/segment/%s/assign/master"
)

type reAssignInfo struct {
	sect  string
	count int
}

func (a *Assign) regSection(ctx context.Context, tag string) error {
	first, last := 0, utils.DoNotChangeHash
	for hashId := first; hashId < last; hashId++ {
		sectKey := fmt.Sprintf("%s/%s/%d", fmt.Sprintf(sectionPrefixFormat, a.table), tag, hashId)
		resp, err := a.client.Txn(ctx).If(
			v3.Compare(v3.Version(sectKey), "=", 0)).
			Then(v3.OpPut(sectKey, "pending")).Commit()
		if err != nil {
			log.Errorf("etcd txn put sectKey %s err %+v", sectKey, err)
			return err
		}
		if resp.Succeeded {
			log.Infof("etcd put %s ok", sectKey)
		}
	}
	return nil
}

func (a *Assign) unRegSection(ctx context.Context, tag string) error {
	prefix := fmt.Sprintf("%s/%s", fmt.Sprintf(sectionPrefixFormat, a.table), tag)
	resp, err := a.client.Delete(ctx, prefix, v3.WithPrefix())
	if err != nil {
		log.Errorf("etcd del prefix %s err %+v", prefix, err)
		return err
	}
	log.Infof("etcd del prefix %s %d", prefix, resp.Deleted)
	return nil
}

type status struct {
	modVersion int64
	timestamp  int64
}

type allocTable struct {
	mutex sync.RWMutex
	table map[string]*status
}

func (a *allocTable) snapshot(ctx context.Context) []string {
	snapshot := make([]string, 0)

	a.mutex.RLock()
	defer a.mutex.RUnlock()

	for k := range a.table {
		snapshot = append(snapshot, k)
	}

	return snapshot
}

func (a *allocTable) putAlloc(ctx context.Context, k, v []byte, modify int64) bool {
	name, ts := parseAlloc(ctx, k, v)
	if len(name) == 0 {
		return false
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()

	st, ok := a.table[name]
	if !ok {
		a.table[name] = &status{
			modVersion: modify,
			timestamp:  ts,
		}
		log.Infof("add alloc %s %d %d", name, modify, ts)
		return true
	}

	if st.modVersion >= modify {
		log.Warnf("put alloc %s old %d >= new %d", name, st.modVersion, modify)
		return false
	}

	log.Warnf("mod alloc %s %d %d -> %d %d", name, st.modVersion, st.timestamp, modify, ts)
	st.modVersion = modify
	st.timestamp = ts
	return false
}

func (a *allocTable) delAlloc(ctx context.Context, k []byte, modify int64) bool {
	name, _ := parseAlloc(ctx, k, nil)
	if len(name) == 0 {
		return false
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()

	st, ok := a.table[name]
	if !ok {
		log.Warnf("del alloc %s %d not found", name, modify)
		return false
	}

	if st.modVersion >= modify {
		log.Warnf("del alloc %s old %d >= new %d", name, st.modVersion, modify)
		return false
	}

	log.Infof("del alloc %s %d %d -> %d", name, st.modVersion, st.timestamp, modify)
	delete(a.table, name)
	return true
}

func (a *Assign) watchAlloc(ctx context.Context) {
	x, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		wch := a.client.Watch(ctx, fmt.Sprintf(allocPrefixFormat, a.table), v3.WithPrefix())
		for wResp := range wch {
			for i := range wResp.Events {
				switch wResp.Events[i].Type {
				case mvccpb.PUT:
					_ = a.alloc.putAlloc(ctx, wResp.Events[i].Kv.Key, wResp.Events[i].Kv.Value, wResp.Events[i].Kv.ModRevision)
				case mvccpb.DELETE:
					ok := a.alloc.delAlloc(ctx, wResp.Events[i].Kv.Key, wResp.Events[i].Kv.ModRevision)
					if ok {
						alloc := extractAlloc(ctx, wResp.Events[i].Kv.Key)
						log.Infof("del old alloc %s from table", alloc)

						prefix := fmt.Sprintf("%s/%s", fmt.Sprintf(routingPrefixFormat, a.table), alloc)
						resp, err := a.client.Delete(ctx, prefix, v3.WithPrefix())
						if err != nil {
							log.Errorf("etcd del prefix %s err %+v", prefix, err)
							a.msgBot.SendMsg(ctx, "assign %s: etcd del prefix %s err %+v", a.name, prefix, err)
						} else {
							if resp.Deleted > 0 {
								log.Infof("etcd del prefix %s ok %d", prefix, resp.Deleted)
							}
						}
					}
				default:
					log.Warnf("unknown event type %v", wResp.Events[i].Type)
				}
			}
		}
	}(x)

	a.watchCancel = append(a.watchCancel, cancel)
}

func (a *Assign) doRetry(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case info := <-a.assignRetry:
				sectKey := fmt.Sprintf("%s/%s", fmt.Sprintf(sectionPrefixFormat, a.table), info.sect)
				resp, err := a.client.Get(ctx, sectKey)
				if err != nil {
					log.Errorf("[retry] etcd get %s err %+v", sectKey, err)
					a.msgBot.SendMsg(ctx, "assign %s: [retry] etcd get %s err %+v", a.name, sectKey, err)
					break
				}

				if resp.Count == 0 {
					break // sect unregister
				}

				if string(resp.Kvs[0].Value) != "pending" {
					break // already done
				}

				alloc, err := a.assign(ctx, info.sect)
				if err != nil {
					log.Errorf("[retry] assign sect %s err %+v", info.sect, err)
					a.msgBot.SendMsg(ctx, "assign %s: [retry] assign sect %s err %+v", a.name, info.sect, err)
					break
				}

				allocKey := fmt.Sprintf("%s/%s", fmt.Sprintf(allocPrefixFormat, a.table), alloc)
				ruleKey := fmt.Sprintf("%s/%s/%s", fmt.Sprintf(routingPrefixFormat, a.table), alloc, info.sect)
				txnResp, err := a.client.Txn(ctx).
					If(v3.Compare(v3.Value(sectKey), "=", "pending"), v3.Compare(v3.Version(allocKey), "!=", 0), v3.Compare(v3.Version(ruleKey), "=", 0)).
					Then(v3.OpPut(sectKey, "running"), v3.OpPut(ruleKey, "pending")).
					Commit()
				if err != nil {
					log.Errorf("[retry] etcd txn change sect %s pending -> running & put rule %s err %+v", info.sect, ruleKey, err)
					a.msgBot.SendMsg(ctx, "assign %s: [retry] etcd txn change sect %s pending -> running & put rule %s err %+v", a.name, info.sect, ruleKey, err)
					break
				}

				if !txnResp.Succeeded {
					log.Warnf("[retry] etcd txn change sect %s pending -> running with alloc %s not succeeded", info.sect, alloc)
					if info.count == 0 {
						info.count++ // one check & retry
						a.assignRetry <- info
					}
					break
				}

				log.Infof("[retry] assign sect %s to alloc %s and put rule %s ok", info.sect, alloc, ruleKey)
			}
		}
	}()
}

func (a *Assign) watchSect(ctx context.Context) {
	x, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		wch := a.client.Watch(ctx, fmt.Sprintf(sectionPrefixFormat, a.table), v3.WithPrefix())
		for wResp := range wch {
			for i := range wResp.Events {
				switch wResp.Events[i].Type {
				case mvccpb.PUT:
					a.onPutSect(ctx, wResp.Events[i].Kv.Key, wResp.Events[i].Kv.Value)
				case mvccpb.DELETE:
					// do nothing
				default:
					log.Warnf("unknown event type %v", wResp.Events[i].Type)
				}
			}
		}
	}(x)

	a.watchCancel = append(a.watchCancel, cancel)
}

func (a *Assign) onPutSect(ctx context.Context, k, v []byte) {
	tag, sect, status := parseSection(ctx, k, v)
	if len(tag) == 0 || len(sect) == 0 || (status != "pending") {
		return
	}

	alloc, err := a.assign(ctx, sect)
	if err != nil {
		log.Errorf("assign sect %s err %+v", sect, err)
		return
	}

	allocKey := fmt.Sprintf("%s/%s", fmt.Sprintf(allocPrefixFormat, a.table), alloc)
	ruleKey := fmt.Sprintf("%s/%s/%s", fmt.Sprintf(routingPrefixFormat, a.table), alloc, sect)
	resp, err := a.client.Txn(ctx).
		If(v3.Compare(v3.Value(string(k)), "=", "pending"), v3.Compare(v3.Version(allocKey), "!=", 0)).
		Then(v3.OpPut(string(k), "running"), v3.OpPut(ruleKey, "pending")).
		Commit()
	if err != nil {
		log.Errorf("etcd txn change sect %s pending -> running & put rule %s err %+v", sect, ruleKey, err)
		a.assignRetry <- &reAssignInfo{
			sect:  sect,
			count: 0,
		}
		return
	}

	if !resp.Succeeded {
		log.Warnf("etcd txn change sect %s pending -> running not succeeded", sect)
		a.assignRetry <- &reAssignInfo{
			sect:  sect,
			count: 0,
		}
		return
	}

	log.Infof("assign sect %s to alloc %s and put rule %s ok", sect, alloc, ruleKey)
}

func (a *Assign) watchRule(ctx context.Context) {
	x, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		wch := a.client.Watch(ctx, fmt.Sprintf(routingPrefixFormat, a.table), v3.WithPrefix())
		for wResp := range wch {
			for i := range wResp.Events {
				switch wResp.Events[i].Type {
				case mvccpb.PUT:
					// do nothing
				case mvccpb.DELETE:
					a.onDelRule(ctx, wResp.Events[i].Kv.Key)
				default:
					log.Warnf("unknown event type %v", wResp.Events[i].Type)
				}
			}
		}
	}(x)

	a.watchCancel = append(a.watchCancel, cancel)
}

func (a *Assign) onDelRule(ctx context.Context, k []byte) {
	alloc, sect := extractRouting(ctx, k)
	if len(alloc) == 0 || len(sect) == 0 {
		return
	}

	sectKey := fmt.Sprintf("%s/%s", fmt.Sprintf(sectionPrefixFormat, a.table), sect)
	resp, err := a.client.Txn(ctx).
		If(v3.Compare(v3.Version(sectKey), "!=", 0), v3.Compare(v3.Value(sectKey), "=", "running")).
		Then(v3.OpPut(sectKey, "pending")).
		Commit()
	if err != nil {
		log.Errorf("etcd txn change sect %s running -> pending err %+v", sect, err)
		a.msgBot.SendMsg(ctx, "assign %s: etcd txn change sect %s running -> pending err %+v ", a.name, sect, err)
		return
	}

	if !resp.Succeeded {
		log.Debugf("etcd txn change sect %s running -> pending not succeeded", sect)
		return
	}

	log.Infof("change sect %s from running -> pending", sect)
}

func (a *Assign) assign(ctx context.Context, sect string) (string, error) {
	snapshot := a.alloc.snapshot(ctx)
	if len(snapshot) == 0 {
		log.Errorf("no alloc to assign")
		return "", utils.ErrAllocNotReady
	}

	return utils.NewHash(ctx, snapshot).Get(ctx, sect), nil
}

func (a *Assign) batchAssign(ctx context.Context, sectList []string) (map[string]string, error) {
	result := make(map[string]string)
	snapshot := a.alloc.snapshot(ctx)
	if len(snapshot) == 0 {
		log.Errorf("no alloc to assign")
		return result, utils.ErrAllocNotReady
	}

	hash := utils.NewHash(ctx, snapshot)
	for i := range sectList {
		alloc := hash.Get(ctx, sectList[i])
		result[sectList[i]] = alloc
	}
	return result, nil
}

func (a *Assign) election(ctx context.Context) {
	ttl := a.electionTTL
	key := fmt.Sprintf(assignMasterFormat, a.table)

	go func() {
		for {
			session, err := concurrency.NewSession(a.client, concurrency.WithTTL(ttl))
			if err != nil {
				log.Errorf("concurrency new session err %+v", err)
				time.Sleep(time.Second * time.Duration(ttl/3))
				continue
			}

			election := concurrency.NewElection(session, key)
			for {
				ok := campaign(ctx, election, a.name)
				if !ok {
					log.Debugf("campaign fail %s", a.name)
					a.master.Store(false)
					_ = session.Close()
					time.Sleep(time.Second * time.Duration(ttl/3))
					break
				}

				a.master.Store(true)
				time.Sleep(time.Second * time.Duration(ttl/3))
			}
		}
	}()
}

func (a *Assign) runCronjob(ctx context.Context) error {
	if len(a.checkPending) != 0 {
		_, err := a.cronjob.AddFunc(a.checkPending, func() {
			if !a.master.Load() {
				return
			}

			prefix := fmt.Sprintf(sectionPrefixFormat, a.table)
			resp, err := a.client.Get(ctx, prefix, v3.WithPrefix())
			if err != nil {
				log.Errorf("etcd get prefix %s err %+v", prefix, err)
				return
			}

			for i := range resp.Kvs {
				a.onPutSect(ctx, resp.Kvs[i].Key, resp.Kvs[i].Value)
			}
		})
		if err != nil {
			log.Errorf("cron add func err %+v", err)
			return err
		}
	}

	if len(a.checkBalance) != 0 {
		_, err := a.cronjob.AddFunc(a.checkBalance, func() {
			if !a.master.Load() {
				return
			}

			prefix := fmt.Sprintf(routingPrefixFormat, a.table)
			resp, err := a.client.Get(ctx, prefix, v3.WithPrefix())
			if err != nil {
				log.Errorf("etcd get prefix %s err %+v", prefix, err)
				return
			}

			snapshot := make(map[string]string)
			sections := make([]string, 0)
			for i := range resp.Kvs {
				alloc, sect := extractRouting(ctx, resp.Kvs[i].Key)
				if len(alloc) == 0 || len(sect) == 0 {
					return
				}
				snapshot[sect] = alloc
				sections = append(sections, sect)
			}

			target, err := a.batchAssign(ctx, sections)
			if err != nil {
				log.Errorf("batch assign err %+v", err)
				return
			}

			reAssign := delta(ctx, target, snapshot)
			for i := 1; i < len(a.snapshots); i++ {
				a.snapshots[i-1] = a.snapshots[i]
			}
			a.snapshots[len(a.snapshots)-1] = reAssign

			for i := range a.snapshots {
				if a.snapshots[i] == nil || len(a.snapshots[i]) < len(sections)/5 {
					return
				}
			}

			for i := 1; i < len(a.snapshots); i++ {
				if !equal(ctx, a.snapshots[i-1], a.snapshots[i]) {
					return
				}
			}

			for sect, change := range reAssign {
				ruleKey := fmt.Sprintf("%s/%s/%s", fmt.Sprintf(routingPrefixFormat, a.table), change[0], sect)
				resp, err := a.client.Txn(ctx).
					If(v3.Compare(v3.Version(ruleKey), "!=", 0)).
					Then(v3.OpDelete(ruleKey)).
					Commit()
				if err != nil {
					log.Errorf("etcd txn delete rule key %s err %+v", ruleKey, err)
					return
				}

				if resp.Succeeded {
					log.Infof("rebalance for sect %s %v", sect, change)
				}
			}
		})
		if err != nil {
			log.Errorf("cron add func err %+v", err)
			return err
		}
	}

	a.cronjob.Start()
	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

func parseAlloc(ctx context.Context, k, v []byte) (string, int64) {
	key := string(k)
	name := key[strings.LastIndex(key, "/")+1:]
	if len(name) == 0 {
		log.Warnf("invalid alloc key %s", key)
		return "", 0
	}

	if v == nil {
		return name, 0
	}

	ts, err := strconv.ParseInt(string(v), 10, 64)
	if err != nil {
		log.Errorf("parse ts %s err %+v", string(v), err)
		return "", 0
	}

	return name, ts
}

func extractAlloc(ctx context.Context, k []byte) string {
	key := string(k)
	name := key[strings.LastIndex(key, "/")+1:]
	return name
}

func parseSection(ctx context.Context, k, v []byte) (string, string, string) {
	content := strings.Split(strings.Trim(string(k), "/"), "/")
	if len(content) != utils.RegSectNum {
		log.Warnf("unknown section key %+v %d", content, len(content))
		return "", "", ""
	}

	return content[utils.RegSectNum-2], fmt.Sprintf("%s/%s", content[utils.RegSectNum-2], content[utils.RegSectNum-1]), string(v)
}

func extractRouting(ctx context.Context, k []byte) (string, string) {
	content := strings.Split(strings.Trim(string(k), "/"), "/")
	if len(content) != utils.RoutingSectNum {
		log.Warnf("unknown routing key %+v %d", content, len(content))
		return "", ""
	}

	sect := fmt.Sprintf("%s/%s", content[utils.RoutingSectNum-2], content[utils.RoutingSectNum-1])
	return content[utils.RoutingSectNum-3], sect
}

func campaign(ctx context.Context, election *concurrency.Election, name string) bool {
	err := election.Campaign(ctx, name)
	if err != nil {
		log.Errorf("campaign err %+v", err)
		return false
	}
	return true
}

func delta(ctx context.Context, target, current map[string]string) map[string][]string {
	result := make(map[string][]string)
	for k, v := range current {
		val, ok := target[k]
		if !ok {
			continue // impossible
		}
		if v != val {
			result[k] = []string{v, val}
		}
	}
	return result
}

func equal(ctx context.Context, m, n map[string][]string) bool {
	if len(m) != len(n) {
		return false
	}

	for k, v := range m {
		val, ok := n[k]
		if !ok {
			return false
		}

		if len(v) != len(val) {
			return false
		}

		for i := range v {
			if val[i] != v[i] {
				return false
			}
		}
	}

	return true
}
