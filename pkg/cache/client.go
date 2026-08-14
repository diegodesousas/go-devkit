package cache

import (
	"context"
	"time"

	"github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"
)

// Client is the raw string-level cache. Values are whatever go-redis can encode
// - strings, byte slices, numbers - and a zero ttl means the key never expires.
//
// Get returns the redis.Nil error when the key is absent, which callers have to
// compare against github.com/redis/go-redis/v9 to tell a miss from a failure.
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

// NewClient connects to the Redis instance at addr ("host:port").
//
// It takes no credentials, database index, TLS config or timeouts, so it does
// not reach an instance that requires authentication.
func NewClient(addr string) Client {
	return &client{
		redis: redis.NewClient(&redis.Options{
			Addr: addr,
		}),
	}
}

// NewRepository returns a Repository storing values of type T as JSON on top of
// client.
func NewRepository[T any](client Client) Repository[T] {
	return &repository[T]{
		Client: client,
	}
}

type repository[T any] struct {
	Client Client
}

// Repository stores values of type T, marshalling to JSON on Put and
// unmarshalling on Get.
//
// Get returns the zero value of T alongside any error, including the redis.Nil
// that signals a miss.
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
