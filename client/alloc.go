package client

import (
	"context"

	pb "github.com/cestlascorpion/sardine/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Alloc struct {
	pb.AllocClient
	cc *grpc.ClientConn
}

func NewAllocClient(target string, opts ...grpc.DialOption) (*Alloc, error) {
	conn, err := grpc.Dial(target, opts...)
	if err != nil {
		log.Errorf("grpc dial %s err %+v", target, err)
		return nil, err
	}
	return &Alloc{
		AllocClient: pb.NewAllocClient(conn),
		cc:          conn,
	}, nil
}

func (c *Alloc) GenUserSeq(ctx context.Context, id uint32, tag string) (int64, error) {
	resp, err := c.AllocClient.GenUserSeq(ctx, &pb.GenUserSeqReq{
		Id:  id,
		Tag: tag,
	})
	if err != nil {
		return 0, err
	}
	return resp.Id, nil
}

func (c *Alloc) GetUserSeq(ctx context.Context, id uint32, tag string) (int64, error) {
	resp, err := c.AllocClient.GetUserSeq(ctx, &pb.GetUserSeqReq{
		Id:  id,
		Tag: tag,
	})
	if err != nil {
		return 0, err
	}
	return resp.Id, nil
}

func (c *Alloc) Close(ctx context.Context) error {
	return c.cc.Close()
}
