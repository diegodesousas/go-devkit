package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithBootstrapServer(t *testing.T) {
	option := WithBootstrapServer("test:9092")

	settings := clientSettings{}

	settings = option(settings)

	expectedSettings := clientSettings{
		bootstrapServer: "test:9092",
	}

	assert.Equal(t, expectedSettings, settings)
}
