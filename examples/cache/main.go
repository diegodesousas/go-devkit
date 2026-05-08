package main

import (
    "context"
    "fmt"
    "time"

    "github.com/diegodesousas/go-devkit/pkg/cache"
)

type ItemTest struct {
    Value  string
    Amount float32
}

func main() {
    client := cache.NewClient("localhost:6379")

    repository := cache.NewRepository[ItemTest](client)

    item := ItemTest{
        Value:  "foo",
        Amount: 13,
    }

    ttl := 10 * time.Second
    if err := repository.Put(context.Background(), "foo", item, ttl); err != nil {
        panic(err)
    }

    recoveredItem, err := repository.Get(context.Background(), "foo")
    if err != nil {
        panic(err)
    }

    fmt.Println(recoveredItem)
}
