package client

import (
	"context"

	pb "github.com/cestlascorpion/sardine/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type Assign struct {
	pb.AssignClient
	cc *grpc.ClientConn
}

func NewAssignClient(target string, opts ...grpc.DialOption) (*Assign, error) {
	conn, err := grpc.Dial(target, opts...)
	if err != nil {
		log.Errorf("grpc dial %s err %+v", target, err)
		return nil, err
	}
	return &Assign{
		AssignClient: pb.NewAssignClient(conn),
		cc:           conn,
	}, nil
}

func (c *Assign) RegSection(ctx context.Context, tag string, async bool) error {
	_, err := c.AssignClient.RegSection(ctx, &pb.RegSectionReq{
		Tag:   tag,
		Async: async,
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *Assign) UnRegSection(ctx context.Context, tag string, async bool) error {
	_, err := c.AssignClient.UnRegSection(ctx, &pb.UnRegSectionReq{
		Tag:   tag,
		Async: async,
	})
	if err != nil {
		return err
	}
	return nil
}

func (c *Assign) Close(ctx context.Context) error {
	return c.cc.Close()
}
