package assign

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

func TestNewAssign(t *testing.T) {
	conf := utils.NewTestConfig()

	assign, err := NewAssign(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	_ = assign.Close(context.Background())
	time.Sleep(time.Second * 2)
}

func TestAssign_RegSection(t *testing.T) {
	conf := utils.NewTestConfig()

	assign, err := NewAssign(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer assign.Close(context.Background())

	err = assign.RegSection(context.Background(), "tag", true)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
}

func TestAssign_UnRegSection(t *testing.T) {
	conf := utils.NewTestConfig()

	assign, err := NewAssign(context.Background(), conf)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer assign.Close(context.Background())

	err = assign.UnRegSection(context.Background(), "tag", true)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
}
