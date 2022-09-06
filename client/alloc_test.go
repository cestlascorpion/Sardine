package client

import (
	"context"
	"fmt"
	"testing"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	testAllocTarget = "127.0.0.1:8100"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

func TestAlloc_GenUserSeq(t *testing.T) {
	client, err := NewAllocClient(testAllocTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	resp, err := client.GenUserSeq(context.Background(), 1234, "tag")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(resp)
}

func TestAlloc_GetUserSeq(t *testing.T) {
	client, err := NewAllocClient(testAllocTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	resp, err := client.GetUserSeq(context.Background(), 1234, "tag")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(resp)
}
