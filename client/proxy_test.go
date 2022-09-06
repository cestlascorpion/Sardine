package client

import (
	"context"
	"fmt"
	"testing"

	"github.com/cestlascorpion/sardine/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	testProxyTarget = "127.0.0.1:8080"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

func TestProxy_GenUserSeq(t *testing.T) {
	client, err := NewProxyClient(testProxyTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func TestProxy_GenUserSeq2(t *testing.T) {
	client, err := NewProxyClient(testProxyTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	for i := 0; i <= int(utils.MaxUserId); i++ {
		seq, err := client.GenUserSeq(context.Background(), uint32(i), "tag")
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		fmt.Println(i, "->", seq)
	}
}

func TestProxy_GetUserSeq(t *testing.T) {
	client, err := NewProxyClient(testProxyTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func TestProxy_GetUserSeq2(t *testing.T) {
	client, err := NewProxyClient(testProxyTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	for i := 0; i <= int(utils.MaxUserId); i++ {
		seq, err := client.GetUserSeq(context.Background(), uint32(i), "tag")
		if err != nil {
			fmt.Println(err)
			t.FailNow()
		}
		fmt.Println(i, "->", seq)
	}
}

func TestProxy_GenUserMultiSeq(t *testing.T) {
	client, err := NewProxyClient(testProxyTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	resp, err := client.GenUserMultiSeq(context.Background(), 1234, []string{"tag_0", "tag_1", "tag_2"}, false)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(resp)

	resp, err = client.GenUserMultiSeq(context.Background(), 1234, []string{"tag_0", "tag_1", "tag_2"}, true)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(resp)
}

func TestProxy_GetUserMultiSeq(t *testing.T) {
	client, err := NewProxyClient(testProxyTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	resp, err := client.GetUserMultiSeq(context.Background(), 1234, []string{"tag_0", "tag_1", "tag_2"})
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(resp)
}

func TestProxy_BatchGenUserSeq(t *testing.T) {
	client, err := NewProxyClient(testProxyTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	resp, err := client.BatchGenUserSeq(context.Background(), []uint32{1, 2, 3, 4}, "tag", false)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(resp)

	resp, err = client.BatchGenUserSeq(context.Background(), []uint32{1, 2, 3, 4}, "tag", true)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(resp)
}

func TestProxy_BatchGetUserSeq(t *testing.T) {
	client, err := NewProxyClient(testProxyTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	resp, err := client.BatchGetUserSeq(context.Background(), []uint32{1, 2, 3, 4}, "tag")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(resp)
}
