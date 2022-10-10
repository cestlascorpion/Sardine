package proxy

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cestlascorpion/sardine/client"
	"github.com/cestlascorpion/sardine/utils"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Proxy watch /segment/{table}/routing/{addr}/{tag}/{hashId}, manage router in order to handler rpc.
type Proxy struct {
	table       string
	name        string
	client      *v3.Client
	watchCancel context.CancelFunc
	mutex       sync.RWMutex
	router      map[string]*alloc
	msgBot      *utils.LarkBot
}

func NewProxy(ctx context.Context, conf *utils.Config) (*Proxy, error) {
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

	proxy := &Proxy{
		table:  conf.GetTable(),
		name:   conf.GetEtcdId(),
		client: cli,
		router: make(map[string]*alloc),
		msgBot: bot,
	}

	prefix := fmt.Sprintf(routingPrefixFormat, proxy.table)
	proxy.watchRule(ctx, prefix)

	resp, err := cli.Get(ctx, prefix, v3.WithPrefix())
	if err != nil {
		log.Errorf("etc get err %+v", err)
		return nil, err
	}

	if resp.Count != 0 {
		for i := range resp.Kvs {
			proxy.putRule(ctx, resp.Kvs[i].Key, resp.Kvs[i].Value, resp.Kvs[i].ModRevision)
		}
	}

	log.Infof("proxy %s ready to go", proxy.name)
	proxy.msgBot.SendMsg(ctx, "proxy %s: ready to go", proxy.name)
	return proxy, nil
}

func (p *Proxy) GenUserSeq(ctx context.Context, id uint32, tag string) (int64, error) {
	seq, err := p.genSeq(ctx, id, tag)
	if err != nil {
		log.Errorf("gen seq %d %s err %+v", id, tag, err)
		return 0, err
	}
	return seq, nil
}

func (p *Proxy) GetUserSeq(ctx context.Context, id uint32, tag string) (int64, error) {
	seq, err := p.getSeq(ctx, id, tag)
	if err != nil {
		log.Errorf("get seq %d %s err %+v", id, tag, err)
		return 0, err
	}
	return seq, nil
}

func (p *Proxy) GenUserMultiSeq(ctx context.Context, id uint32, tagList []string, async bool) (map[string]int64, error) {
	if async {
		go func() {
			_, err := p.genUserMultiSeq(context.Background(), id, tagList)
			if err != nil {
				log.Errorf("GenUserMultiSeq id %d tagList %v async err %+v", id, tagList, err)
			}
		}()
		return nil, nil
	}
	return p.genUserMultiSeq(ctx, id, tagList)
}

func (p *Proxy) GetUserMultiSeq(ctx context.Context, id uint32, tagList []string) (map[string]int64, error) {
	res := make([]*multiResult, len(tagList))
	var wg sync.WaitGroup
	wg.Add(len(tagList))

	for i := range tagList {
		go func(ctx context.Context, id uint32, tag string, idx int) {
			defer wg.Done()

			seq, err := p.getSeq(ctx, id, tag)
			if err != nil {
				log.Errorf("get seq %d %s err %+v", id, tag, err)
				return
			}
			res[idx] = &multiResult{
				tag: tag,
				seq: seq,
			}
			log.Debugf("get user seq for id %d tag %s seq %d", id, tag, seq)
		}(ctx, id, tagList[i], i)
	}

	wg.Wait()
	result := make(map[string]int64)
	for i := range res {
		if res[i] == nil {
			continue
		}
		result[res[i].tag] = res[i].seq
	}

	return result, nil
}

func (p *Proxy) BatchGenUserSeq(ctx context.Context, idList []uint32, tag string, async bool) (map[uint32]int64, error) {
	if async {
		go func() {
			_, err := p.batchGenUserSeq(context.Background(), idList, tag)
			if err != nil {
				log.Errorf("GenUserMultiSeq idList %v tag %s async err %+v", idList, tag, err)
			}
		}()
		return nil, nil
	}
	return p.batchGenUserSeq(ctx, idList, tag)
}

func (p *Proxy) BatchGetUserSeq(ctx context.Context, idList []uint32, tag string) (map[uint32]int64, error) {
	res := make([]*batchResult, len(idList))
	var wg sync.WaitGroup
	wg.Add(len(idList))

	for i := range idList {
		go func(ctx context.Context, id uint32, tag string, idx int) {
			defer wg.Done()

			seq, err := p.getSeq(ctx, id, tag)
			if err != nil {
				log.Errorf("get seq %d %s err %+v", id, tag, err)
				return
			}
			res[idx] = &batchResult{
				id:  id,
				seq: seq,
			}
			log.Debugf("get user seq for id %d tag %s seq %d", id, tag, seq)
		}(ctx, idList[i], tag, i)
	}

	wg.Wait()
	result := make(map[uint32]int64)
	for i := range res {
		if res[i] == nil {
			continue
		}
		result[res[i].id] = res[i].seq
	}
	return result, nil
}

func (p *Proxy) Close(ctx context.Context) error {
	p.watchCancel()
	log.Infof("cancel watch go routine")

	return nil
}

// ---------------------------------------------------------------------------------------------------------------------

const (
	routingPrefixFormat = "/segment/%s/routing"
)

func (p *Proxy) getAlloc(id uint32, tag string) (*client.Alloc, error) {
	key := fmt.Sprintf("%s/%d", tag, id%utils.DoNotChangeHash)

	p.mutex.RLock()
	defer p.mutex.RUnlock()

	value, ok := p.router[key]
	if !ok {
		return nil, utils.ErrNoRoutingFound
	}
	return value.client, nil
}

type multiResult struct {
	tag string
	seq int64
}

func (p *Proxy) genUserMultiSeq(ctx context.Context, id uint32, tagList []string) (map[string]int64, error) {
	res := make([]*multiResult, len(tagList))
	var wg sync.WaitGroup
	wg.Add(len(tagList))

	for i := range tagList {
		go func(ctx context.Context, id uint32, tag string, idx int) {
			defer wg.Done()

			seq, err := p.genSeq(ctx, id, tag)
			if err != nil {
				log.Errorf("gen seq %d %s err %+v", id, tag, err)
				return
			}
			res[idx] = &multiResult{
				tag: tag,
				seq: seq,
			}
			log.Debugf("gen user seq for id %d tag %s seq %d", id, tag, seq)
		}(ctx, id, tagList[i], i)
	}

	wg.Wait()
	result := make(map[string]int64)
	for i := range res {
		if res[i] == nil {
			continue
		}
		result[res[i].tag] = res[i].seq
	}
	return result, nil
}

type batchResult struct {
	id  uint32
	seq int64
}

func (p *Proxy) batchGenUserSeq(ctx context.Context, idList []uint32, tag string) (map[uint32]int64, error) {
	res := make([]*batchResult, len(idList))
	var wg sync.WaitGroup
	wg.Add(len(idList))

	for i := range idList {
		go func(ctx context.Context, id uint32, tag string, idx int) {
			defer wg.Done()

			seq, err := p.genSeq(ctx, id, tag)
			if err != nil {
				log.Errorf("gen seq %d %s err %+v", id, tag, err)
				return
			}
			res[idx] = &batchResult{
				id:  id,
				seq: seq,
			}
			log.Debugf("gen user seq for id %d tag %s seq %d", id, tag, seq)
		}(ctx, idList[i], tag, i)
	}

	wg.Wait()
	result := make(map[uint32]int64)
	for i := range res {
		if res[i] == nil {
			continue
		}
		result[res[i].id] = res[i].seq
	}
	return result, nil
}

func (p *Proxy) genSeq(ctx context.Context, id uint32, tag string) (int64, error) {
	var (
		cli *client.Alloc
		seq int64
		err error
	)

	for try := 1; try <= utils.MaxRetryNum; try++ {
		cli, err = p.getAlloc(id, tag)
		if err == nil {
			seq, err = cli.GenUserSeq(ctx, id, tag)
			if err == nil {
				return seq, nil
			}
		}
		if try < utils.MaxRetryNum {
			time.Sleep(utils.RetryInterval * time.Duration(try))
		}
	}
	return seq, err
}

func (p *Proxy) getSeq(ctx context.Context, id uint32, tag string) (int64, error) {
	var (
		cli *client.Alloc
		seq int64
		err error
	)

	for try := 1; try <= utils.MaxRetryNum; try++ {
		cli, err = p.getAlloc(id, tag)
		if err == nil {
			seq, err = cli.GetUserSeq(ctx, id, tag)
			if err == nil {
				return seq, nil
			}
		}
		if try < utils.MaxRetryNum {
			time.Sleep(utils.RetryInterval * time.Duration(try))
		}
	}
	return seq, err
}

func (p *Proxy) watchRule(ctx context.Context, prefix string) {
	x, cancel := context.WithCancel(ctx)

	go func(ctx context.Context) {
		wch := p.client.Watch(ctx, prefix, v3.WithPrefix(), v3.WithPrevKV())
		for wResp := range wch {
			for i := range wResp.Events {
				switch wResp.Events[i].Type {
				case mvccpb.PUT:
					p.putRule(ctx, wResp.Events[i].Kv.Key, wResp.Events[i].Kv.Value, wResp.Events[i].Kv.ModRevision)
				case mvccpb.DELETE:
					p.delRule(ctx, wResp.Events[i].Kv.Key, wResp.Events[i].PrevKv.Value, wResp.Events[i].Kv.ModRevision)
				}
			}
		}
	}(x)

	p.watchCancel = cancel
}

type alloc struct {
	modVersion int64
	client     *client.Alloc
}

func (p *Proxy) putRule(ctx context.Context, k, v []byte, modify int64) {
	if string(v) != "running" {
		return
	}

	addr, sect := parseRouting(ctx, string(k))
	if len(addr) == 0 || len(sect) == 0 {
		return
	}

	cli, err := client.NewAllocClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Errorf("get client for %s err %+v", addr, err)
		return
	}

	value := &alloc{
		modVersion: modify,
		client:     cli,
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	old, ok := p.router[sect]
	if !ok {
		p.router[sect] = value
		log.Infof("add rule %s %s %d", addr, sect, modify)
		return
	}

	if old.modVersion >= value.modVersion {
		log.Warnf("put rule %s %s old %d >= new %d", addr, sect, old.modVersion, modify)
		p.msgBot.SendMsg(ctx, "[SYS BUG] proxy %s: put rule %s %s old %d >= new %d", p.name, addr, sect, old.modVersion, modify)
		return
	}

	p.router[sect] = value
	log.Warnf("mod rule %s %s %+v", addr, sect, modify)
	p.msgBot.SendMsg(ctx, "[SYS BUG] proxy %s: mod rule %s %s %+v", p.name, addr, sect, modify)
}

func (p *Proxy) delRule(ctx context.Context, k, v []byte, modify int64) {
	if string(v) != "running" {
		return
	}

	addr, sect := parseRouting(ctx, string(k))
	if len(addr) == 0 || len(sect) == 0 {
		return
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	old, ok := p.router[sect]
	if !ok {
		log.Warnf("del rule %s %s %d not found", addr, sect, modify)
		p.msgBot.SendMsg(ctx, "[SYS BUG] proxy %s: del rule %s %s %d not found", p.name, addr, sect, modify)
		return
	}

	if old.modVersion >= modify {
		log.Warnf("del rule %s %s old %d >= new %d", addr, sect, old.modVersion, modify)
		p.msgBot.SendMsg(ctx, "[SYS BUG] proxy %s: del rule %s %s old %d >= new %d", p.name, addr, sect, old.modVersion, modify)
		return
	}

	old.client.Close(ctx)
	delete(p.router, sect)
	log.Infof("del rule %s %s %d", addr, sect, modify)
}

// ---------------------------------------------------------------------------------------------------------------------

func parseRouting(ctx context.Context, key string) (string, string) {
	content := strings.Split(strings.Trim(key, "/"), "/")
	if len(content) != utils.RoutingSectNum {
		log.Warnf("unknown routing key %s", key)
		return "", ""
	}
	return content[utils.RoutingSectNum-3], fmt.Sprintf("%s/%s", content[utils.RoutingSectNum-2], content[utils.RoutingSectNum-1])
}
