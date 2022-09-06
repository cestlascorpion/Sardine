package storage

import (
	"context"
)

type Store interface {
	UpdateMaxId(ctx context.Context, key string) (int64, error)
	Close(ctx context.Context) error
}
