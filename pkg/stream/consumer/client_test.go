package consumer_test

import (
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/stream/consumer"
    "github.com/stretchr/testify/assert"
)

func TestNewFactory_Success(t *testing.T) {
    factory := consumer.NewFactory()

    assert.NotNil(t, factory)
}

func TestNewFactory_SuccessWithOptions(t *testing.T) {
    factory := consumer.NewFactory(
        consumer.WithBootstrapServer("localhost:9092"),
    )

    assert.NotNil(t, factory)
}

func TestNewFactory_New_Success(t *testing.T) {
    factory := consumer.NewFactory()

    client, err := factory.New("test")

    assert.NotNil(t, client)
    assert.Nil(t, err)
}
