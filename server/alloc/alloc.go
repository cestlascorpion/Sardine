package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/cestlascorpion/sardine/proto"
	"github.com/cestlascorpion/sardine/service/alloc"
	"github.com/cestlascorpion/sardine/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	port int
)

func init() {
	flag.IntVar(&port, "port", 0, "listen port")
}

func main() {
	conf, err := utils.NewConfig(context.Background(), "./config.yaml")
	if err != nil {
		log.Fatalf("new config failed err %+v", err)
		return
	}

	flag.Parse()
	if port != 0 {
		conf.Server.Addr = fmt.Sprintf(":%d", port)
	}

	lg, err := log.ParseLevel(conf.Server.LogLevel)
	if err != nil {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(lg)
	}

	lis, err := net.Listen("tcp4", conf.Server.Addr)
	if err != nil {
		log.Fatalf("listen failed err %+v", err)
		return
	}

	svr, err := alloc.NewAllocServer(context.Background(), conf)
	if err != nil {
		log.Fatalf("new server failed err %+v", err)
		return
	}

	s := grpc.NewServer()
	pb.RegisterAllocServer(s, svr)
	reflection.Register(s)

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)

	go func() {
		err = s.Serve(lis)
		if err != nil {
			log.Fatalf("serve failed err %+v", err)
			signal.Notify(wait, syscall.SIGTERM)
		}
	}()

	<-wait
	s.GracefulStop()
	_ = svr.Close(context.Background())

	time.Sleep(time.Second * 2)
	log.Infof("server exit ...")
}
