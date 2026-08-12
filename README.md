# go-devkit

Go developer kit for multipurpose applications

# Streams

## Dispatcher

- Graceful Shutdown
- Messages in JSON format
- Message with delivery confirmation

For an example see [basic dispatcher](./examples/stream/dispatcher/main.go)

## Consumer

- Graceful Shutdown
- Messages in JSON format
- Retry option with exponential backoff
- Automatic sending of error messages to dead letter

For an example see [basic consumer](./examples/stream/consumer/main.go)

## HTTP Server

- Graceful Shutdown
- Easy routes configuration
- Middlewares

For an example see [basic http server](./examples/httpserver/main.go)

## Logger

- Leveled Logger
- Logger with fields
- Formatted Json Logger
- Logger being passed by context

For an example see [basic logging](./examples/log/main.go)

## Cache

- Basic getter and setter cache level

For an example see [basic cache](./examples/cache/main.go)
