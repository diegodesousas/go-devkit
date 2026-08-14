package cache_test

import (
	"context"
	"fmt"
	"time"

	"github.com/diegodesousas/go-devkit/pkg/cache"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

type session struct {
	UserID string `json:"user_id"`
	Admin  bool   `json:"admin"`
}

// Requires a Redis instance, so this example is compiled but not run.
func ExampleNewRepository() {
	client := cache.NewClient("localhost:6379")
	sessions := cache.NewRepository[session](client)

	ctx := context.Background()

	if err := sessions.Put(ctx, "sess:42", session{UserID: "42"}, 10*time.Minute); err != nil {
		panic(err)
	}

	s, err := sessions.Get(ctx, "sess:42")
	if err != nil {
		panic(err)
	}

	fmt.Println(s.UserID)
}

// A miss is not distinguished from a failure by this package: Get returns the
// driver's redis.Nil, so telling them apart means comparing against it.
func ExampleRepository_miss() {
	sessions := cache.NewRepository[session](cache.NewClient("localhost:6379"))

	_, err := sessions.Get(context.Background(), "sess:absent")

	switch {
	case errors.Is(err, redis.Nil):
		fmt.Println("cache miss")
	case err != nil:
		fmt.Println("cache failure:", err)
	default:
		fmt.Println("hit")
	}
}
