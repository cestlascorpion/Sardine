package alloc

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

func TestNewAlloc(t *testing.T) {
	conf := utils.NewTestConfig()

	proxy, err := NewAlloc(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	time.Sleep(time.Second * 20)
	_ = proxy.Close(context.Background())
	time.Sleep(time.Second * 2)
}

func TestSegment_GenUserSeq(t *testing.T) {
	conf := utils.NewTestConfig()

	proxy, err := NewAlloc(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer proxy.Close(context.Background())

	time.Sleep(time.Second * 20)
	resp, err := proxy.GenUserSeq(context.Background(), 1234, "tag")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(resp)
}

func TestSegment_GetUserSeq(t *testing.T) {
	conf := utils.NewTestConfig()

	proxy, err := NewAlloc(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer proxy.Close(context.Background())

	time.Sleep(time.Second * 20)
	resp, err := proxy.GetUserSeq(context.Background(), 1234, "tag")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(resp)
}
