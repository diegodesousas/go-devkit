package dispatcher

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithBootstrapServers_Success(t *testing.T) {
	option := WithBootstrapServers("test:9999")

	settings := dispatcherSettings{}

	settings = option(settings)

	expectedSettings := dispatcherSettings{
		bootstrapServers: "test:9999",
	}

	assert.Equal(t, expectedSettings, settings)
}

func TestNewClient_Success(t *testing.T) {
	client, err := NewClient(
		WithLogLevel(0),
	)

	assert.NotNil(t, client)
	assert.Nil(t, err)
}

func TestNewClient_Success_WithOptions(t *testing.T) {
	client, err := NewClient(
		WithBootstrapServers("test:9999"),
		WithLogLevel(0),
	)

	assert.NotNil(t, client)
	assert.Nil(t, err)
}

func TestWithLogLevel_Success(t *testing.T) {
	option := WithLogLevel(3)

	settings := dispatcherSettings{}

	settings = option(settings)

	expectedSettings := dispatcherSettings{
		logLevel: 3,
	}

	assert.Equal(t, expectedSettings, settings)
}

func TestWithDispatchTimeoutMs_Success(t *testing.T) {
	option := WithDispatchTimeoutMs(10000)

	settings := dispatcherSettings{}

	settings = option(settings)

	expectedSettings := dispatcherSettings{
		messageTimeoutMs: 10000,
	}

	assert.Equal(t, expectedSettings, settings)
}
