.PHONY: test test-integration test-all fmt lint

test:
	go test ./pkg/... -race

test-integration:
	go test ./pkg/database/sql/... -race -tags=integration -count=1

test-all: test test-integration

fmt:
	gofmt -w ./pkg ./examples

lint:
	gofmt -l ./pkg ./examples
	go vet ./...
	golangci-lint run --timeout=5m
