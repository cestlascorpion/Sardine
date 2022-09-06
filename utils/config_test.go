package utils

import (
	"testing"
)

func TestConfig(t *testing.T) {
	config := NewTestConfig()
	if !config.Check() {
		t.FailNow()
	}
}
