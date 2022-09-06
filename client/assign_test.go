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
	testAssignTarget = "127.0.0.1:8090"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

func TestAssign_RegSection(t *testing.T) {
	client, err := NewAssignClient(testAssignTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	err = client.RegSection(context.Background(), "tag", true)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
}

func TestAssign_UnRegSection(t *testing.T) {
	client, err := NewAssignClient(testAssignTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	err = client.UnRegSection(context.Background(), "tag", true)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
}

func TestAssign_RegSection2(t *testing.T) {
	client, err := NewAssignClient(testAssignTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	err = client.RegSection(context.Background(), "tag_0", false)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
}

func TestAssign_UnRegSection2(t *testing.T) {
	client, err := NewAssignClient(testAssignTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	err = client.UnRegSection(context.Background(), "tag_0", false)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
}

func TestAssign_RegSection3(t *testing.T) {
	client, err := NewAssignClient(testAssignTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	err = client.RegSection(context.Background(), "tag_1", false)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
}

func TestAssign_UnRegSection3(t *testing.T) {
	client, err := NewAssignClient(testAssignTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	err = client.UnRegSection(context.Background(), "tag_1", false)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
}

func TestAssign_RegSection4(t *testing.T) {
	client, err := NewAssignClient(testAssignTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	err = client.RegSection(context.Background(), "tag_2", false)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
}

func TestAssign_UnRegSection4(t *testing.T) {
	client, err := NewAssignClient(testAssignTarget, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}

	err = client.UnRegSection(context.Background(), "tag_2", false)
	if err != nil {
		fmt.Println(err)
		t.FailNow()
	}
}
