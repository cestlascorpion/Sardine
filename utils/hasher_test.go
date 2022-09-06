package utils

import (
	"context"
	"fmt"
	"testing"
)

func TestHash_Get(t *testing.T) {
	keys := []string{"A", "B", "C", "D", "E", "F"}
	hash := NewHash(context.Background(), keys)

	countMap := make(map[string]int)
	for i := 0; i < 10240; i++ {
		key := hash.Get(context.Background(), fmt.Sprintf("key-%d", i))
		countMap[key]++
	}
	fmt.Println(countMap)
}
