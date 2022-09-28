package client

import (
	"context"

	pb "github.com/cestlascorpion/sardine/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Proxy struct {
	pb.ProxyClient
}

func NewProxyClient(target string, opts ...grpc.DialOption) (*Proxy, error) {
	conn, err := grpc.Dial(target, opts...)
	if err != nil {
		log.Errorf("grpc dial %s err %+v", target, err)
		return nil, err
	}
	return &Proxy{
		ProxyClient: pb.NewProxyClient(conn),
	}, nil
}

func (c *Proxy) GenUserSeq(ctx context.Context, id uint32, tag string) (int64, error) {
	resp, err := c.ProxyClient.GenUserSeq(ctx, &pb.GenUserSeqReq{
		Id:  id,
		Tag: tag,
	})
	if err != nil {
		return 0, err
	}
	return resp.Id, nil
}

func (c *Proxy) GetUserSeq(ctx context.Context, id uint32, tag string) (int64, error) {
	resp, err := c.ProxyClient.GetUserSeq(ctx, &pb.GetUserSeqReq{
		Id:  id,
		Tag: tag,
	})
	if err != nil {
		return 0, err
	}
	return resp.Id, nil
}

func (c *Proxy) GenUserMultiSeq(ctx context.Context, id uint32, tagList []string, async bool) (map[string]int64, error) {
	resp, err := c.ProxyClient.GenUserMultiSeq(ctx, &pb.GenUserMultiSeqReq{
		Id:      id,
		TagList: tagList,
		Async:   async,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetSeqList(), nil
}

func (c *Proxy) GetUserMultiSeq(ctx context.Context, id uint32, tagList []string) (map[string]int64, error) {
	resp, err := c.ProxyClient.GetUserMultiSeq(ctx, &pb.GetUserMultiSeqReq{
		Id:      id,
		TagList: tagList,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetSeqList(), nil
}

func (c *Proxy) BatchGenUserSeq(ctx context.Context, idList []uint32, tag string, async bool) (map[uint32]int64, error) {
	resp, err := c.ProxyClient.BatchGenUserSeq(ctx, &pb.BatchGenUserSeqReq{
		IdList: idList,
		Tag:    tag,
		Async:  async,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetSeqList(), nil
}

func (c *Proxy) BatchGetUserSeq(ctx context.Context, idList []uint32, tag string) (map[uint32]int64, error) {
	resp, err := c.ProxyClient.BatchGetUserSeq(ctx, &pb.BatchGetUserSeqReq{
		IdList: idList,
		Tag:    tag,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetSeqList(), nil
}
