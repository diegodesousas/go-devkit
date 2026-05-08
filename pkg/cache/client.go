package cache

import (
	"context"
	"time"

	"github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
)

type Client interface {
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	Get(ctx context.Context, key string) (string, error)
}

type client struct {
	redis *redis.Client
}

func (c client) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	return c.redis.Set(ctx, key, value, ttl).Err()
}

func (c client) Get(ctx context.Context, key string) (string, error) {
	return c.redis.Get(ctx, key).Result()
}

func NewClient(addr string) Client {
	return &client{
		redis: redis.NewClient(&redis.Options{
			Addr: addr,
		}),
	}
}

func NewRepository[T any](client Client) Repository[T] {
	return &repository[T]{
		Client: client,
	}
}

type repository[T any] struct {
	Client Client
}

type Repository[T any] interface {
	Get(ctx context.Context, key string) (T, error)
	Put(ctx context.Context, key string, t T, ttl time.Duration) error
}

func (c repository[T]) Get(ctx context.Context, key string) (T, error) {
	var item T
	result, err := c.Client.Get(ctx, key)
	if err != nil {
		return item, err
	}

	if err := json.Unmarshal([]byte(result), &item); err != nil {
		return item, err
	}

	return item, nil
}

func (c repository[T]) Put(ctx context.Context, key string, t T, ttl time.Duration) error {
	content, err := json.Marshal(t)
	if err != nil {
		return err
	}

	if err := c.Client.Set(ctx, key, content, ttl); err != nil {
		return err
	}

	return nil
}
