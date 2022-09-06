package proxy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cestlascorpion/sardine/utils"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

func TestNewProxy(t *testing.T) {
	conf := utils.NewTestConfig()

	proxy, err := NewProxy(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	_ = proxy.Close(context.Background())
	time.Sleep(time.Second * 2)
}

func TestImpl_GenUserSeq(t *testing.T) {
	conf := utils.NewTestConfig()

	proxy, err := NewProxy(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer proxy.Close(context.Background())

	seq, err := proxy.GenUserSeq(context.Background(), 1234, "tag")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(seq)
}

func TestImpl_GetUserSeq(t *testing.T) {
	conf := utils.NewTestConfig()

	proxy, err := NewProxy(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer proxy.Close(context.Background())

	seq, err := proxy.GetUserSeq(context.Background(), 1234, "tag")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(seq)
}

func TestImpl_GenUserMultiSeq(t *testing.T) {
	conf := utils.NewTestConfig()

	proxy, err := NewProxy(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer proxy.Close(context.Background())

	seqMap, err := proxy.GenUserMultiSeq(context.Background(), 1234, []string{"tag_0", "tag_1", "tag_2"}, false)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(seqMap)
}

func TestImpl_GetUserMultiSeq(t *testing.T) {
	conf := utils.NewTestConfig()

	proxy, err := NewProxy(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer proxy.Close(context.Background())

	seqMap, err := proxy.GetUserMultiSeq(context.Background(), 1234, []string{"tag_0", "tag_1", "tag_2"})
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(seqMap)
}

func TestImpl_BatchGenUserSeq(t *testing.T) {
	conf := utils.NewTestConfig()

	proxy, err := NewProxy(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer proxy.Close(context.Background())

	seqMap, err := proxy.BatchGenUserSeq(context.Background(), []uint32{1, 2, 3, 4}, "tag", false)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(seqMap)
}

func TestImpl_BatchGetUserSeq(t *testing.T) {
	conf := utils.NewTestConfig()

	proxy, err := NewProxy(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer proxy.Close(context.Background())

	seqMap, err := proxy.BatchGetUserSeq(context.Background(), []uint32{1, 2, 3, 4}, "tag")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(seqMap)
}
