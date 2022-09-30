package alloc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cestlascorpion/sardine/storage"
	"github.com/cestlascorpion/sardine/utils"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
)

// Alloc lease /segment/{table}/alloc/{addr}, watch /segment/{table}/routing/{addr}/{tag}/{hashId}
// handle pending routing rule, set value 'running' if the 'pending' rule is ready to serve. If alloc goes
// down, it will try to delete lease key and delete --prefix routing key at once; assign will do the same
// in case alloc crashed.
type Alloc struct {
	table       string
	name        string
	client      *v3.Client
	watchCancel []context.CancelFunc
	leaseCancel context.CancelFunc
	mutex       sync.RWMutex
	cache       map[string]*section
	retry       chan string
	store       storage.Store
	msgBot      *utils.LarkBot
}

func NewAlloc(ctx context.Context, conf *utils.Config) (*Alloc, error) {
	bot, err := utils.NewLarkBot(ctx, conf)
	if err != nil {
		log.Errorf("new lark bot err %+v", err)
		return nil, err
	}

	var store storage.Store
	switch conf.GetType() {
	case utils.TypeRedis:
		st, err := storage.NewRedis(ctx, conf)
		if err != nil {
			log.Errorf("storage new redis err %+v", err)
			return nil, err
		}
		store = st
	case utils.TypeMysql:
		st, err := storage.NewMySQL(ctx, conf)
		if err != nil {
			log.Errorf("storage new mysql err %+v", err)
			return nil, err
		}
		store = st
	default:
		log.Errorf("invalid store type %d", conf.GetType())
		return nil, utils.ErrInvalidParameter
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

	alloc := &Alloc{
		table:  conf.GetTable(),
		name:   conf.GetEtcdId(),
		client: cli,
		cache:  make(map[string]*section),
		retry:  make(chan string, 256),
		store:  store,
		msgBot: bot,
	}

	prefix := fmt.Sprintf(routingPrefixFormat, alloc.table, alloc.name)
	resp, err := cli.Get(ctx, prefix, v3.WithPrefix())
	if err != nil {
		log.Errorf("etcd get err %+v", err)
		return nil, err
	}

	if resp.Count != 0 {
		log.Errorf("unexpected rules %d", resp.Count)
		return nil, utils.ErrUnexpectedRules
	}

	alloc.doRetry(ctx)
	alloc.watchRule(ctx, prefix)
	alloc.watchSect(ctx)

	key := fmt.Sprintf(allocKeyFormat, alloc.table, alloc.name)
	err = alloc.lease(ctx, key)
	if err != nil {
		log.Errorf("alloc lease err %+v", err)
		return nil, err
	}

	log.Infof("alloc %s ready to go", alloc.name)
	alloc.msgBot.SendMsg(ctx, "alloc %s: ready to go", alloc.name)
	return alloc, nil
}

func (a *Alloc) GenUserSeq(ctx context.Context, id uint32, tag string) (int64, error) {
	key := fmt.Sprintf("%s/%d", tag, id%utils.DoNotChangeHash)
	seg, err := a.getSection(ctx, key)
	if err != nil {
		log.Errorf("get section for id %d tag %s err %+v", id, tag, err)
		return 0, err
	}

	for try := 0; try < maxRetry; try++ {
		seq, ok := seg.genUserSeq(ctx, id)
		if ok {
			return seq, nil
		}

		err = seg.updateMaxId(ctx, key, a.store, seq)
		if err != nil {
			log.Warnf("update max id for id %d tag %s err %+v", id, tag, err)
			time.Sleep(time.Millisecond * 5)
		}
	}

	return 0, utils.ErrSectionNotReady
}

func (a *Alloc) GetUserSeq(ctx context.Context, id uint32, tag string) (int64, error) {
	key := fmt.Sprintf("%s/%d", tag, id%utils.DoNotChangeHash)
	seg, err := a.getSection(ctx, key)
	if err != nil {
		log.Errorf("get section for id %d tag %s err %+v", id, tag, err)
		return 0, err
	}

	return seg.getUserSeq(ctx, id), nil
}

func (a *Alloc) Close(ctx context.Context) error {
	for i := range a.watchCancel {
		a.watchCancel[i]()
	}
	log.Infof("cancel watch go routine")

	a.leaseCancel()
	log.Infof("cancel lease go routine")

	prefix := fmt.Sprintf(routingPrefixFormat, a.table, a.name)
	_, err := a.client.Delete(ctx, prefix, v3.WithPrefix())
	if err != nil {
		log.Errorf("etcd del prefix %s err %+v", prefix, err)
		a.msgBot.SendMsg(ctx, "alloc %s: etcd del prefix %s err %+v", a.name, prefix, err)
	} else {
		log.Infof("etcd del prefix %s at once", prefix)
	}

	key := fmt.Sprintf(allocKeyFormat, a.table, a.name)
	_, err = a.client.Delete(ctx, key)
	if err != nil {
		log.Errorf("etcd del key %s err %+v", prefix, err)
		a.msgBot.SendMsg(ctx, "alloc %s: etcd del key %s err %+v", a.name, prefix, err)
	} else {
		log.Infof("etcd del key %s at once", prefix)
	}

	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

const (
	maxRetry            = 3
	allocKeyFormat      = "/segment/%s/alloc/%s"
	sectionPrefixFormat = "/segment/%s/section"
	routingPrefixFormat = "/segment/%s/routing/%s"
)

func (a *Alloc) getSection(ctx context.Context, key string) (*section, error) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	seg, ok := a.cache[key]
	if !ok {
		return nil, utils.ErrSectionNotReady
	}
	return seg, nil
}

func (a *Alloc) doRetry(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case sect := <-a.retry:
				_, err := a.getSection(ctx, sect)
				if err != utils.ErrSectionNotReady {
					log.Warnf("[retry] section %s is serving", sect)
					break
				}

				maxSeq, err := a.store.UpdateMaxId(ctx, sect)
				if err != nil {
					log.Errorf("[retry] update max id for %s err %+v", sect, err)
					a.msgBot.SendMsg(ctx, "alloc %s: [retry] update max id for %s err +v", a.name, sect, err)
					break
				}

				seg := newSection(maxSeq, maxSeq-utils.DoNotChangeStep-1)

				a.mutex.Lock()
				a.cache[sect] = seg
				a.mutex.Unlock()

				routingKey := fmt.Sprintf("%s/%s", fmt.Sprintf(routingPrefixFormat, a.table, a.name), sect)
				_, err = a.client.Txn(ctx).If(
					v3.Compare(v3.Value(routingKey), "=", "pending"),
				).Then(
					v3.OpPut(routingKey, "running"),
				).Commit()

				if err != nil {
					a.mutex.Lock()
					delete(a.cache, sect)
					a.mutex.Unlock()
					log.Errorf("[retry] etcd txn change key %s pending -> running err %+v", routingKey, err)
					a.msgBot.SendMsg(ctx, "alloc %s: [retry] Txn %s pending -> running err %+v", a.name, routingKey, err)
					break
				}

				log.Infof("[retry] set cache for %s", sect)
			}
		}
	}()
}

func (a *Alloc) watchRule(ctx context.Context, prefix string) {
	x, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		wch := a.client.Watch(ctx, prefix, v3.WithPrefix())
		for wResp := range wch {
			for i := range wResp.Events {
				switch wResp.Events[i].Type {
				case mvccpb.PUT:
					a.putRule(ctx, wResp.Events[i].Kv.Key, wResp.Events[i].Kv.Value)
				case mvccpb.DELETE:
					a.delRule(ctx, wResp.Events[i].Kv.Key)
				default:
					log.Warnf("unknown event type %v", wResp.Events[i].Type)
				}
			}
		}
	}(x)

	a.watchCancel = append(a.watchCancel, cancel)
}

func (a *Alloc) putRule(ctx context.Context, k, v []byte) {
	if string(v) != "pending" {
		return
	}

	addr, sect := parseRouting(ctx, string(k))
	if len(addr) == 0 || len(sect) == 0 {
		return
	}

	if addr != a.name {
		return
	}

	_, err := a.getSection(ctx, sect)
	if err != utils.ErrSectionNotReady {
		log.Warnf("section %s is serving", sect)
		return
	}

	maxSeq, err := a.store.UpdateMaxId(ctx, sect)
	if err != nil {
		log.Errorf("update max id for %s err %+v", sect, err)
		a.retry <- sect
		return
	}

	seg := newSection(maxSeq, maxSeq-utils.DoNotChangeStep-1)

	a.mutex.Lock()
	a.cache[sect] = seg
	a.mutex.Unlock()

	routingKey := fmt.Sprintf("%s/%s", fmt.Sprintf(routingPrefixFormat, a.table, a.name), sect)
	_, err = a.client.Txn(ctx).If(
		v3.Compare(v3.Value(routingKey), "=", "pending"),
	).Then(
		v3.OpPut(routingKey, "running"),
	).Commit()

	if err != nil {
		a.mutex.Lock()
		delete(a.cache, sect)
		a.mutex.Unlock()
		log.Errorf("etcd txn change key %s pending -> running err %+v", routingKey, err)
		a.retry <- sect
		return
	}

	log.Infof("set cache for %s", sect)
}

func (a *Alloc) delRule(ctx context.Context, k []byte) {
	addr, sect := parseRouting(ctx, string(k))
	if len(addr) == 0 || len(sect) == 0 {
		return
	}

	if addr != a.name {
		return
	}

	_, err := a.getSection(ctx, sect)
	if err != nil {
		log.Warnf("section %s is not serving", sect)
		return
	}

	a.mutex.Lock()
	delete(a.cache, sect)
	a.mutex.Unlock()

	log.Infof("del cache for %s", sect)
}

func (a *Alloc) watchSect(ctx context.Context) {
	x, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		wch := a.client.Watch(ctx, fmt.Sprintf(sectionPrefixFormat, a.table), v3.WithPrefix())
		for wResp := range wch {
			for i := range wResp.Events {
				switch wResp.Events[i].Type {
				case mvccpb.PUT:
					// do nothing
				case mvccpb.DELETE:
					a.delSect(ctx, wResp.Events[i].Kv.Key)
				default:
					log.Warnf("unknown event type %v", wResp.Events[i].Type)
				}
			}
		}
	}(x)

	a.watchCancel = append(a.watchCancel, cancel)
}

func (a *Alloc) delSect(ctx context.Context, k []byte) {
	sect := extractSection(ctx, k)
	if len(sect) == 0 {
		return
	}

	_, err := a.getSection(ctx, sect)
	if err == utils.ErrSectionNotReady {
		return
	}

	a.mutex.Lock()
	delete(a.cache, sect)
	a.mutex.Unlock()
	log.Infof("del cache for %s", sect)

	ruleKey := fmt.Sprintf("%s/%s", fmt.Sprintf(routingPrefixFormat, a.table, a.name), sect)
	resp, err := a.client.Delete(ctx, ruleKey)
	if err != nil {
		log.Errorf("etcd del key %s err %+v", ruleKey, err)
		return
	}

	log.Infof("etcd del key %s %d", ruleKey, resp.Deleted)
}

func (a *Alloc) lease(ctx context.Context, key string) error {
	leaseResp, err := a.client.Grant(ctx, 12)
	if err != nil {
		log.Errorf("etcd grant err %+v", err)
		return err
	}

	txnResp, err := a.client.Txn(ctx).If(
		v3.Compare(v3.Version(key), "=", 0),
	).Then(
		v3.OpPut(key, fmt.Sprintf("%d", time.Now().UnixMilli()), v3.WithLease(leaseResp.ID)),
	).Commit()
	if err != nil {
		log.Errorf("etcd txn create key %s err %+v", key, err)
		return err
	}

	if !txnResp.Succeeded {
		log.Errorf("key %s already exists", key)
		return utils.ErrAllocExists
	}

	leaseChan, err := a.client.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		log.Errorf("etcd keep alive err %+v", err)
		return err
	}

	x, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-leaseChan:
				if !ok {
					log.Errorf("etcd lease chan closed resp %+v", resp)
					return
				}
			}
		}
	}(x)

	a.leaseCancel = cancel

	return nil
}

type section struct {
	curSeq []atomic.Int64
	maxSeq int64
	mutex  sync.RWMutex
}

func newSection(maxSeq, curSeq int64) *section {
	seg := &section{
		curSeq: make([]atomic.Int64, (utils.MaxUserId+utils.DoNotChangeStep-1)/utils.DoNotChangeHash),
		maxSeq: maxSeq,
	}
	for i := range seg.curSeq {
		seg.curSeq[i].Store(curSeq)
	}
	return seg
}

func (s *section) updateMaxId(ctx context.Context, key string, store storage.Store, last int64) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	max := s.maxSeq
	if max > last {
		log.Debugf("updated by another go routine")
		return nil
	}

	max, err := store.UpdateMaxId(ctx, key)
	if err != nil {
		log.Errorf("update max id for %s err %+v", key, err)
		return err
	}

	s.maxSeq = max
	return nil
}

func (s *section) genUserSeq(ctx context.Context, id uint32) (int64, bool) {
	s.mutex.RLock()
	max := s.maxSeq
	s.mutex.RUnlock()

	idx := id / utils.DoNotChangeHash
	next := s.curSeq[idx].Add(1)

	if next < max {
		return next, true
	}
	return max, false
}

func (s *section) getUserSeq(ctx context.Context, id uint32) int64 {
	idx := id / utils.DoNotChangeHash
	return s.curSeq[idx].Load()
}

// ---------------------------------------------------------------------------------------------------------------------

func extractSection(ctx context.Context, k []byte) string {
	content := strings.Split(strings.Trim(string(k), "/"), "/")
	if len(content) != utils.RegSectNum {
		log.Warnf("unknown section key %+v %d", content, len(content))
		return ""
	}

	return fmt.Sprintf("%s/%s", content[utils.RegSectNum-2], content[utils.RegSectNum-1])
}

func parseRouting(ctx context.Context, key string) (string, string) {
	content := strings.Split(strings.Trim(key, "/"), "/")
	if len(content) != utils.RoutingSectNum {
		log.Warnf("unknown routing key %s", key)
		return "", ""
	}
	return content[utils.RoutingSectNum-3], fmt.Sprintf("%s/%s", content[utils.RoutingSectNum-2], content[utils.RoutingSectNum-1])
}
