package sql_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/diegodesousas/go-devkit/pkg/database/sql"
	"github.com/stretchr/testify/assert"
)

func TestConfig_String_RedactsPassword(t *testing.T) {
	var (
		expectedPassword = "sup3r-s3cr3t"
		expectedConfig   = sql.Config{
			Host:     "localhost",
			Port:     "5432",
			User:     "postgres",
			Password: expectedPassword,
			Database: "devkit",
			SSLMode:  "disable",
		}
		expectedString = "host=localhost port=5432 user=postgres password=**** dbname=devkit sslmode=disable"
	)

	tests := []struct {
		name  string
		value string
	}{
		{
			name:  "String()",
			value: expectedConfig.String(),
		},
		{
			name:  "%v verb",
			value: fmt.Sprintf("%v", expectedConfig),
		},
		{
			name:  "nested in a struct",
			value: fmt.Sprintf("%v", struct{ Config sql.Config }{Config: expectedConfig}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotContains(t, tt.value, expectedPassword)
			assert.Contains(t, tt.value, "password=****")
		})
	}

	assert.Equal(t, expectedString, expectedConfig.String())
}

func TestConfig_String_KeepsEmptyPasswordEmpty(t *testing.T) {
	var (
		expectedConfig = sql.Config{
			Host:     "localhost",
			Port:     "5432",
			User:     "postgres",
			Database: "devkit",
			SSLMode:  "disable",
		}
		expectedString = "host=localhost port=5432 user=postgres password= dbname=devkit sslmode=disable"
	)

	assert.Equal(t, expectedString, expectedConfig.String())
	assert.False(t, strings.Contains(expectedConfig.String(), "****"))
}
