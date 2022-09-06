package alloc

import (
	"context"

	pb "github.com/cestlascorpion/sardine/proto"
	"github.com/cestlascorpion/sardine/utils"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	*pb.UnimplementedAllocServer
	impl *Alloc
}

func NewAllocServer(ctx context.Context, conf *utils.Config) (*Server, error) {
	if conf == nil || !conf.Check() {
		log.Errorf("invalid config")
		return nil, utils.ErrInvalidParameter
	}

	a, err := NewAlloc(ctx, conf)
	if err != nil {
		log.Errorf("new alloc impl err %+v", err)
		return nil, err
	}

	return &Server{
		impl: a,
	}, nil
}

func (a *Server) GenUserSeq(ctx context.Context, in *pb.GenUserSeqReq) (*pb.GenUserSeqResp, error) {
	out := &pb.GenUserSeqResp{}
	log.Debugf("GenUserSeq in %+v", in)

	seq, err := a.impl.GenUserSeq(ctx, in.Id, in.Tag)
	if err != nil {
		log.Errorf("impl GenUserSeq err %+v", err)
		return out, err
	}

	out.Id = seq
	return out, nil
}

func (a *Server) GetUserSeq(ctx context.Context, in *pb.GetUserSeqReq) (*pb.GetUserSeqResp, error) {
	out := &pb.GetUserSeqResp{}
	log.Debugf("GetUserSeq in %+v", in)

	seq, err := a.impl.GetUserSeq(ctx, in.Id, in.Tag)
	if err != nil {
		log.Errorf("impl GetUserSeq err %+v", err)
		return out, err
	}

	out.Id = seq
	return out, nil
}

func (a *Server) Close(ctx context.Context) error {
	return a.impl.Close(ctx)
}
