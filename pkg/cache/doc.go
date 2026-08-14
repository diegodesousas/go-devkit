// Package cache stores values in Redis, either as raw strings or as
// JSON-encoded Go values.
//
// Client is the low level: it sets and gets strings under a TTL.
//
//	client := cache.NewClient("localhost:6379")
//
// Repository is the typed layer on top. It marshals on the way in and
// unmarshals on the way out, so callers work with their own types:
//
//	orders := cache.NewRepository[Order](client)
//
//	if err := orders.Put(ctx, key, order, 10*time.Minute); err != nil {
//		return err
//	}
//
//	order, err := orders.Get(ctx, key)
//
// A key that is absent is not special-cased here: Get returns the redis.Nil
// error from the underlying driver, so distinguishing a miss from a real
// failure currently means importing github.com/redis/go-redis/v9 and comparing
// against it.
//
// NewClient takes an address and nothing else - no password, database index,
// TLS or timeouts - so it reaches a local or sidecar Redis but not a managed
// instance that requires authentication.
package cache
