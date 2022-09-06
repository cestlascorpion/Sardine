package utils

import (
	"context"
	"fmt"
	"testing"
)

func TestLarkBot_SendMsg(t *testing.T) {
	config := NewTestConfig()
	bot, err := NewLarkBot(context.Background(), config)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	bot.SendMsg(context.Background(), "what does fox say: %s", "nope")
}
