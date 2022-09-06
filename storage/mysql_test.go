package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/cestlascorpion/sardine/utils"
)

func TestMySQL_UpdateMaxId(t *testing.T) {
	sql, err := NewMySQL(context.Background(), utils.NewTestConfig())
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	defer sql.Close(context.Background())

	maxId, err := sql.UpdateMaxId(context.Background(), "A/0")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(maxId)

	maxId, err = sql.UpdateMaxId(context.Background(), "A/0")
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
	fmt.Println(maxId)
}
