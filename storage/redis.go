package storage

import (
	"context"
	"fmt"

	"github.com/cestlascorpion/sardine/utils"
	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type Redis struct {
	client *redis.Client
	prefix string
}

func NewRedis(ctx context.Context, conf *utils.Config) (*Redis, error) {
	client := redis.NewClient(&redis.Options{
		Network:  conf.Storage.Redis.Network,
		Addr:     conf.Storage.Redis.Addr,
		Username: conf.Storage.Redis.Username,
		Password: conf.Storage.Redis.Password,
		DB:       conf.Storage.Redis.Database,
	})
	err := client.Ping(ctx).Err()
	if err != nil {
		log.Errorf("redis ping err %+v", err)
		return nil, err
	}

	return &Redis{
		client: client,
		prefix: fmt.Sprintf("%s@%s", conf.GetTable(), conf.Storage.Redis.KeyPrefix),
	}, nil
}

func (r *Redis) UpdateMaxId(ctx context.Context, tag string) (int64, error) {
	key := fmt.Sprintf(redisKeyFormat, r.prefix, tag)

	maxId, err := r.client.IncrBy(ctx, key, utils.DoNotChangeStep).Result()
	if err != nil {
		log.Errorf("redis incr %s %+v", key, err)
		return 0, err
	}

	return maxId, nil
}

func (r *Redis) Close(ctx context.Context) error {
	return r.client.Close()
}

// ---------------------------------------------------------------------------------------------------------------------

const (
	redisKeyFormat = "%s@%s"
)

// ---------------------------------------------------------------------------------------------------------------------
