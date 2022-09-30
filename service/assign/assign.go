package assign

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cestlascorpion/sardine/utils"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
)

// Assign watch /segment/{table}/alloc/{addr} and /segment/{table}/section/{tag}/{hashId}.
// assign 'pending' section to a certain alloc and put routing rule(with 'pending' value). delete --prefix
// routing key in case alloc crashed. watch routing rule to manage idle section to re-assign.
type Assign struct {
	table       string
	name        string
	client      *v3.Client
	alloc       *allocTable
	reCheck     chan []byte
	checkCancel context.CancelFunc
	watchCancel []context.CancelFunc
	snapshots   []map[string][]string
	msgBot      *utils.LarkBot
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
		table:   conf.GetTable(),
		name:    conf.GetEtcdId(),
		client:  cli,
		reCheck: make(chan []byte, 1024),
		alloc: &allocTable{
			table: make(map[string]*status),
		},
		snapshots: make([]map[string][]string, 3),
		msgBot:    bot,
	}

	assign.watchAlloc(ctx)

	prefix := fmt.Sprintf(allocPrefixFormat, assign.table)
	resp, err := cli.Get(ctx, prefix, v3.WithPrefix())
	if err != nil {
		log.Errorf("etcd get err %+v", err)
		return nil, err
	}

	for i := range resp.Kvs {
		assign.putAlloc(ctx, resp.Kvs[i].Key, resp.Kvs[i].Value, resp.Kvs[i].ModRevision)
	}

	assign.doCheck(ctx)
	assign.watchSect(ctx)
	assign.watchRule(ctx)

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
	for i := range a.watchCancel {
		a.watchCancel[i]()
	}
	log.Infof("cancel watch go routine")

	a.checkCancel()
	log.Infof("cancel re-check go routine")
	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

const (
	allocPrefixFormat   = "/segment/%s/alloc"
	sectionPrefixFormat = "/segment/%s/section"
	routingPrefixFormat = "/segment/%s/routing"
)

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

func (a *Assign) snapshot(ctx context.Context) []string {
	snapshot := make([]string, 0)

	a.alloc.mutex.RLock()
	defer a.alloc.mutex.RUnlock()

	for k := range a.alloc.table {
		snapshot = append(snapshot, k)
	}

	return snapshot
}

func (a *Assign) putAlloc(ctx context.Context, k, v []byte, modify int64) bool {
	name, ts := parseAlloc(ctx, k, v)
	if len(name) == 0 {
		return false
	}

	a.alloc.mutex.Lock()
	defer a.alloc.mutex.Unlock()

	st, ok := a.alloc.table[name]
	if !ok {
		a.alloc.table[name] = &status{
			modVersion: modify,
			timestamp:  ts,
		}
		log.Infof("add alloc %s %d %d", name, modify, ts)
		return true
	}

	if st.modVersion >= modify {
		log.Warnf("put alloc %s old %d >= new %d", name, st.modVersion, modify)
		a.msgBot.SendMsg(ctx, "[SYS BUG] assign %s: put alloc %s old %d >= new %d", a.name, name, st.modVersion, modify)
		return false
	}

	log.Warnf("mod alloc %s %d %d -> %d %d", name, st.modVersion, st.timestamp, modify, ts)
	a.msgBot.SendMsg(ctx, "[SYS BUG] assign %s: mod alloc %s %d %d -> %d %d", a.name, name, st.modVersion, st.timestamp, modify, ts)
	st.modVersion = modify
	st.timestamp = ts
	return false
}

func (a *Assign) delAlloc(ctx context.Context, k, v []byte, modify int64) bool {
	name, _ := parseAlloc(ctx, k, nil)
	if len(name) == 0 {
		return false
	}

	a.alloc.mutex.Lock()
	defer a.alloc.mutex.Unlock()

	st, ok := a.alloc.table[name]
	if !ok {
		log.Warnf("del alloc %s %d not found", name, modify)
		a.msgBot.SendMsg(ctx, "[SYS BUG] assign %s: del alloc %s %d not found", a.name, name, modify)
		return false
	}

	if st.modVersion >= modify {
		log.Warnf("del alloc %s old %d >= new %d", name, st.modVersion, modify)
		a.msgBot.SendMsg(ctx, "[SYS BUG] assign %s: del alloc %s old %d >= new %d", a.name, name, st.modVersion, modify)
		return false
	}

	log.Infof("del alloc %s %d %d -> %d", name, st.modVersion, st.timestamp, modify)
	delete(a.alloc.table, name)
	return true
}

func (a *Assign) watchAlloc(ctx context.Context) {
	x, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		wch := a.client.Watch(ctx, fmt.Sprintf(allocPrefixFormat, a.table), v3.WithPrefix(), v3.WithPrevKV())
		for wResp := range wch {
			for i := range wResp.Events {
				switch wResp.Events[i].Type {
				case mvccpb.PUT:
					_ = a.putAlloc(ctx, wResp.Events[i].Kv.Key, wResp.Events[i].Kv.Value, wResp.Events[i].Kv.ModRevision)
				case mvccpb.DELETE:
					ok := a.delAlloc(ctx, wResp.Events[i].Kv.Key, wResp.Events[i].PrevKv.Value, wResp.Events[i].Kv.ModRevision)
					if ok {
						alloc := extractAlloc(ctx, wResp.Events[i].Kv.Key)
						log.Infof("del old alloc %s from table", alloc)

						prefix := fmt.Sprintf("%s/%s", fmt.Sprintf(routingPrefixFormat, a.table), alloc)
						resp, err := a.client.Delete(ctx, prefix, v3.WithPrefix())
						if err != nil {
							log.Errorf("etcd del prefix %s err %+v", prefix, err)
							a.msgBot.SendMsg(ctx, "[ETCD BUG] assign %s: etcd del prefix %s err %+v", a.name, prefix, err)
						} else {
							if resp.Deleted > 0 {
								log.Infof("DO DELETE %s --prefix %d", prefix, resp.Deleted)
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

func (a *Assign) doCheck(ctx context.Context) {
	x, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case k := <-a.reCheck:
				go a.checkSect(ctx, k)
			}
		}
	}(x)

	a.checkCancel = cancel
}

func (a *Assign) checkSect(ctx context.Context, k []byte) {
	sectKey := string(k)
	tag, sect := extractSection(ctx, k)
	if len(tag) == 0 || len(sect) == 0 {
		return
	}

	time.Sleep(time.Second * 5)
	sectResp, err := a.client.Get(ctx, sectKey)
	if err != nil {
		log.Errorf("etcd get %s err %+v", sectKey, err)
		a.msgBot.SendMsg(ctx, "[ETCD BUG] assign %s: etcd get %s err %+v", a.name, sectKey, err)
		a.reCheck <- k
		return
	}

	if sectResp.Count == 0 {
		log.Infof("sect key %s not exists", sectKey)
		return
	}
	if string(sectResp.Kvs[0].Value) != "pending" {
		log.Debugf("sect key %s is pending", sectKey)
		return
	}

	alloc, err := a.assign(ctx, sect)
	if err != nil {
		log.Errorf("assign sect %s err %+v", sect, err)
		a.msgBot.SendMsg(ctx, "[SYS BUG] assign %s: assign %s err %+v", a.name, sect, err)
		a.reCheck <- k
		return
	}

	allocKey := fmt.Sprintf("%s/%s", fmt.Sprintf(allocPrefixFormat, a.table), alloc)
	ruleKey := fmt.Sprintf("%s/%s/%s", fmt.Sprintf(routingPrefixFormat, a.table), alloc, sect)
	txnResp, err := a.client.Txn(ctx).
		If(v3.Compare(v3.Value(sectKey), "=", "pending"), v3.Compare(v3.Version(allocKey), "!=", 0), v3.Compare(v3.Version(ruleKey), "=", 0)).
		Then(v3.OpPut(sectKey, alloc), v3.OpPut(ruleKey, "pending")).
		Commit()
	if err != nil {
		log.Errorf("etcd txn change sect %s pending -> %s & put rule %s err %+v", sectKey, alloc, ruleKey, err)
		a.msgBot.SendMsg(ctx, "[ETCD BUG] assign %s: etcd txn change sect %s pending -> %s & put rule %s err %+v", a.name, sectKey, alloc, ruleKey, err)
		a.reCheck <- k
		return
	}

	if !txnResp.Succeeded {
		log.Warnf("assign txn not succeeded for %s pending -> %s & put rule %s", sectKey, alloc, ruleKey)
		a.reCheck <- k
		return
	}

	log.Infof("DO TXN PUT %s %s & PUT %s pending IF %s pending & %s exists & %s not exists", sectKey, alloc, ruleKey, sectKey, allocKey, ruleKey)
	log.Infof("assign sect %s to alloc %s and put rule %s ok", sect, alloc, ruleKey)
}

func (a *Assign) watchSect(ctx context.Context) {
	x, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		wch := a.client.Watch(ctx, fmt.Sprintf(sectionPrefixFormat, a.table), v3.WithPrefix(), v3.WithPrevKV())
		for wResp := range wch {
			for i := range wResp.Events {
				switch wResp.Events[i].Type {
				case mvccpb.PUT:
					a.putSect(ctx, wResp.Events[i].Kv.Key, wResp.Events[i].Kv.Value, wResp.Events[i].Kv.ModRevision)
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

func (a *Assign) putSect(ctx context.Context, k, v []byte, modify int64) {
	sectKey := string(k)
	tag, sect, status := parseSection(ctx, k, v)
	if len(tag) == 0 || len(sect) == 0 || (status != "pending") {
		return
	}

	alloc, err := a.assign(ctx, sect)
	if err != nil {
		a.reCheck <- k
		//log.Errorf("assign sect %s err %+v", sect, err)
		return
	}

	allocKey := fmt.Sprintf("%s/%s", fmt.Sprintf(allocPrefixFormat, a.table), alloc)
	ruleKey := fmt.Sprintf("%s/%s/%s", fmt.Sprintf(routingPrefixFormat, a.table), alloc, sect)
	resp, err := a.client.Txn(ctx).
		If(v3.Compare(v3.Value(sectKey), "=", "pending"), v3.Compare(v3.Version(allocKey), "!=", 0), v3.Compare(v3.Version(ruleKey), "=", 0)).
		Then(v3.OpPut(sectKey, alloc), v3.OpPut(ruleKey, "pending")).
		Commit()
	if err != nil {
		log.Errorf("etcd txn change sect %s %d pending -> %s & put rule %s err %+v", sect, modify, alloc, ruleKey, err)
		a.msgBot.SendMsg(ctx, "[ETCD BUG] assign %s: etcd txn change sect %s pending -> %s & put rule %s err %+v", a.name, sect, alloc, ruleKey, err)
		return
	}

	if !resp.Succeeded {
		a.reCheck <- k
		return
	}

	log.Infof("DO TXN PUT %s %s & PUT %s pending IF %s pending & %s exists & %s not exists", sectKey, alloc, ruleKey, sectKey, allocKey, ruleKey)
	log.Infof("assign sect %s %d to alloc %s and put rule %s ok", sect, modify, alloc, ruleKey)
}

func (a *Assign) watchRule(ctx context.Context) {
	x, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		wch := a.client.Watch(ctx, fmt.Sprintf(routingPrefixFormat, a.table), v3.WithPrefix(), v3.WithPrevKV())
		for wResp := range wch {
			for i := range wResp.Events {
				switch wResp.Events[i].Type {
				case mvccpb.PUT:
					// do nothing
				case mvccpb.DELETE:
					a.delRule(ctx, wResp.Events[i].Kv.Key, wResp.Events[i].PrevKv.Value, wResp.Events[i].Kv.ModRevision)
				default:
					log.Warnf("unknown event type %v", wResp.Events[i].Type)
				}
			}
		}
	}(x)

	a.watchCancel = append(a.watchCancel, cancel)
}

func (a *Assign) delRule(ctx context.Context, k, v []byte, modify int64) {
	alloc, sect := extractRouting(ctx, k)
	if len(alloc) == 0 || len(sect) == 0 {
		return
	}

	sectKey := fmt.Sprintf("%s/%s", fmt.Sprintf(sectionPrefixFormat, a.table), sect)
	resp, err := a.client.Txn(ctx).
		If(v3.Compare(v3.Value(sectKey), "=", alloc)).
		Then(v3.OpPut(sectKey, "pending")).
		Commit()
	if err != nil {
		log.Errorf("etcd txn change sect %s %d %s -> pending err %+v", sect, modify, alloc, err)
		a.msgBot.SendMsg(ctx, "[ETCD BUG] assign %s: etcd txn change sect %s %s -> pending err %+v ", a.name, sect, alloc, err)
		return
	}

	if !resp.Succeeded {
		log.Warnf("etcd txn change sect %s %d %s -> pending not succeeded", sect, modify, alloc)
		return
	}

	log.Infof("DO TXN PUT %s pending IF %s %s", sectKey, sectKey, alloc)
	log.Infof("change sect %s %d from %s -> pending", sect, modify, alloc)
}

func (a *Assign) assign(ctx context.Context, sect string) (string, error) {
	snapshot := a.snapshot(ctx)
	if len(snapshot) == 0 {
		log.Errorf("no alloc to assign")
		return "", utils.ErrAllocNotReady
	}

	return utils.NewHash(ctx, snapshot).Get(ctx, sect), nil
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

func extractSection(ctx context.Context, k []byte) (string, string) {
	content := strings.Split(strings.Trim(string(k), "/"), "/")
	if len(content) != utils.RegSectNum {
		log.Warnf("unknown section key %+v %d", content, len(content))
		return "", ""
	}
	return content[utils.RegSectNum-2], fmt.Sprintf("%s/%s", content[utils.RegSectNum-2], content[utils.RegSectNum-1])
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
