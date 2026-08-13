package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/diegodesousas/go-devkit/pkg/stream"
    "github.com/diegodesousas/go-devkit/pkg/stream/dispatcher"
)

type Message struct {
    ID     string `json:"id"`
    Test   bool   `json:"test"`
    Amount int    `json:"amount"`
}

func main() {
    client, err := dispatcher.NewClient(
        dispatcher.WithBootstrapServers("localhost:9092"),
    )
    if err != nil {
        log.Fatal(err)
    }

    d := dispatcher.New(client)

    event := Message{
        ID:     "00",
        Test:   true,
        Amount: 100,
    }

    topic := "godevkit_test-message"

    go func() {
        tick := time.Tick(3 * time.Second)

        for {
            <-tick

            log.Printf("Try dispatch message")
            if err := d.Dispatch(context.Background(), topic, event.ID, stream.NewJSONMessage(event)); err != nil {
                log.Print(err)
                continue
            }

            log.Printf("Message dispatched")
        }

    }()

    interrupt := make(chan os.Signal, 1)
    signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

    <-interrupt
    d.Shutdown()
}
