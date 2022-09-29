package proxy

import (
	"context"
	"time"

	pb "github.com/cestlascorpion/sardine/proto"
	"github.com/cestlascorpion/sardine/utils"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	*pb.UnimplementedProxyServer
	impl *Proxy
}

func NewProxyServer(ctx context.Context, conf *utils.Config) (*Server, error) {
	if conf == nil || !conf.Check() {
		log.Errorf("invalid config")
		return nil, utils.ErrInvalidParameter
	}

	p, err := NewProxy(ctx, conf)
	if err != nil {
		log.Errorf("new proxy impl err %+v", err)
		return nil, err
	}

	return &Server{
		impl: p,
	}, nil
}

func (p *Server) GenUserSeq(ctx context.Context, in *pb.GenUserSeqReq) (*pb.GenUserSeqResp, error) {
	out := &pb.GenUserSeqResp{}
	log.Debugf("GenUserSeq in %+v", in)

	if len(in.Tag) == 0 {
		log.Errorf("invalid tag %s", in.Tag)
		return out, utils.ErrInvalidParameter
	}

	seq, err := p.impl.GenUserSeq(ctx, in.Id, in.Tag)
	if err == utils.ErrNoRoutingFound {
		time.Sleep(time.Millisecond * 3)
		seq, err = p.impl.GenUserSeq(ctx, in.Id, in.Tag)
	}
	if err != nil {
		log.Errorf("impl GenUserSeq err %+v", err)
		return out, err
	}
	out.Id = seq
	return out, nil
}

func (p *Server) GetUserSeq(ctx context.Context, in *pb.GetUserSeqReq) (*pb.GetUserSeqResp, error) {
	out := &pb.GetUserSeqResp{}
	log.Debugf("GetUserSeq in %+v", in)

	if len(in.Tag) == 0 {
		log.Errorf("invalid tag %s", in.Tag)
		return out, utils.ErrInvalidParameter
	}

	seq, err := p.impl.GetUserSeq(ctx, in.Id, in.Tag)
	if err == utils.ErrNoRoutingFound {
		time.Sleep(time.Millisecond * 3)
		seq, err = p.impl.GetUserSeq(ctx, in.Id, in.Tag)
	}
	if err != nil {
		log.Errorf("impl GetUserSeq err %+v", err)
		return out, err
	}
	out.Id = seq
	return out, nil
}

func (p *Server) GenUserMultiSeq(ctx context.Context, in *pb.GenUserMultiSeqReq) (*pb.GenUserMultiSeqResp, error) {
	out := &pb.GenUserMultiSeqResp{}
	log.Debugf("GenUserMultiSeq in %+v", in)

	if len(in.TagList) == 0 {
		log.Errorf("invalid tagList %v", in.TagList)
		return out, utils.ErrInvalidParameter
	}

	seqList, err := p.impl.GenUserMultiSeq(ctx, in.Id, in.TagList, in.Async)
	if err != nil {
		log.Errorf("impl GenUserMultiSeq err %+v", err)
		return out, err
	}
	out.SeqList = seqList
	return out, nil
}

func (p *Server) GetUserMultiSeq(ctx context.Context, in *pb.GetUserMultiSeqReq) (*pb.GetUserMultiSeqResp, error) {
	out := &pb.GetUserMultiSeqResp{}
	log.Debugf("GetUserMultiSeq in %+v", in)

	if len(in.TagList) == 0 {
		log.Errorf("invalid tagList %v", in.TagList)
		return out, utils.ErrInvalidParameter
	}

	seqList, err := p.impl.GetUserMultiSeq(ctx, in.Id, in.TagList)
	if err != nil {
		log.Errorf("impl GetUserMultiSeq err %+v", err)
		return out, err
	}
	out.SeqList = seqList
	return out, nil
}

func (p *Server) BatchGenUserSeq(ctx context.Context, in *pb.BatchGenUserSeqReq) (*pb.BatchGenUserSeqResp, error) {
	out := &pb.BatchGenUserSeqResp{}
	log.Debugf("BatchGenUserSeq in %+v", in)

	if len(in.IdList) == 0 {
		log.Errorf("invalid idList %v", in.IdList)
		return out, utils.ErrInvalidParameter
	}

	if len(in.Tag) == 0 {
		log.Errorf("invalid tag %s", in.Tag)
		return out, utils.ErrInvalidParameter
	}

	seqList, err := p.impl.BatchGenUserSeq(ctx, in.IdList, in.Tag, in.Async)
	if err != nil {
		log.Errorf("impl BatchGenUserSeq err %+v", err)
		return out, err
	}
	out.SeqList = seqList
	return out, nil
}

func (p *Server) BatchGetUserSeq(ctx context.Context, in *pb.BatchGetUserSeqReq) (*pb.BatchGetUserSeqResp, error) {
	out := &pb.BatchGetUserSeqResp{}
	log.Debugf("BatchGetUserSeq in %+v", in)

	if len(in.IdList) == 0 {
		log.Errorf("invalid idList %v", in.IdList)
		return out, utils.ErrInvalidParameter
	}

	if len(in.Tag) == 0 {
		log.Errorf("invalid tag %s", in.Tag)
		return out, utils.ErrInvalidParameter
	}

	seqList, err := p.impl.BatchGetUserSeq(ctx, in.IdList, in.Tag)
	if err != nil {
		log.Errorf("impl BatchGetUserSeq err %+v", err)
		return out, err
	}
	out.SeqList = seqList
	return out, nil
}

func (p *Server) Close(ctx context.Context) error {
	return p.impl.Close(ctx)
}
