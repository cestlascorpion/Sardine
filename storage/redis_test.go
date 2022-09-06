package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/cestlascorpion/sardine/utils"
)

func TestRedis_UpdateMaxId(t *testing.T) {
	rds, err := NewRedis(context.Background(), utils.NewTestConfig())
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer rds.Close(context.Background())

	maxId, err := rds.UpdateMaxId(context.Background(), "A/0")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(maxId)

	maxId, err = rds.UpdateMaxId(context.Background(), "A/0")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(maxId)
}
