package utils

import (
	"errors"
	"math"
)

var (
	ErrInvalidParameter = errors.New("invalid parameter")
	ErrNoRoutingFound   = errors.New("no routing found")
	ErrSectionNotReady  = errors.New("section not ready")
	ErrAllocExists      = errors.New("alloc exists")
	ErrUnexpectedRules  = errors.New("unexpected rules")
	ErrAllocNotReady    = errors.New("alloc not ready")
)

const (
	MinUserId       = int64(0)
	MaxUserId       = int64(math.MaxInt32)
	DoNotChangeHash = 512
	DoNotChangeStep = 10 * 10000
	RegSectNum      = 5
	RoutingSectNum  = 6
)

const (
	defaultStoreType = TypeRedis
	defaultPrefix    = "seq"
	defaultAllocEnv  = "MY_POD_IP"
)

type StorageType int

const (
	TypeRedis StorageType = 1
	TypeMysql StorageType = 2
)
