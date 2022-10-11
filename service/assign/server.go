package assign

import (
	"context"

	pb "github.com/cestlascorpion/sardine/proto"
	"github.com/cestlascorpion/sardine/utils"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	*pb.UnimplementedAssignServer
	impl *Assign
}

func NewAssignServer(ctx context.Context, conf *utils.Config) (*Server, error) {
	if conf == nil || !conf.Check() {
		log.Errorf("invalid config")
		return nil, utils.ErrInvalidParameter
	}

	a, err := NewAssign(ctx, conf)
	if err != nil {
		log.Errorf("new assign impl err %+v", err)
		return nil, err
	}

	return &Server{
		impl: a,
	}, nil
}

func (a *Server) RegSection(ctx context.Context, in *pb.RegSectionReq) (*pb.RegSectionResp, error) {
	out := &pb.RegSectionResp{}
	log.Debugf("RegSection in %+v", in)

	if len(in.Tag) == 0 {
		log.Errorf("invalid tag %s", in.Tag)
		return out, utils.ErrInvalidParameter
	}

	err := a.impl.RegSection(ctx, in.Tag, in.Async)
	if err != nil {
		log.Errorf("impl RegSection err %+v", err)
		return out, err
	}

	log.Infof("RegSection tag %s", in.Tag)
	return out, nil
}

func (a *Server) UnRegSection(ctx context.Context, in *pb.UnRegSectionReq) (*pb.UnRegSectionResp, error) {
	out := &pb.UnRegSectionResp{}
	log.Debugf("UnRegSection in %+v", in)

	if len(in.Tag) == 0 {
		log.Errorf("invalid tag %s", in.Tag)
		return out, utils.ErrInvalidParameter
	}

	err := a.impl.UnRegSection(ctx, in.Tag, in.Async)
	if err != nil {
		log.Errorf("impl UnRegSection err %+v", err)
		return out, err
	}

	log.Infof("UnRegSection tag %s", in.Tag)
	return out, nil
}

func (a *Server) ReBalance(ctx context.Context, in *pb.ReBalanceReq) (*pb.ReBalanceResp, error) {
	out := &pb.ReBalanceResp{}
	log.Debugf("ReBalance in %+v", in)

	err := a.impl.ReBalance(ctx)
	if err != nil {
		log.Errorf("impl ReBalance err %+v", err)
		return out, err
	}
	return out, nil
}

func (a *Server) Close(ctx context.Context) error {
	return a.impl.Close(ctx)
}
