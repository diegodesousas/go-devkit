package kafka

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSettings_Defaults(t *testing.T) {
	s := defaultSettings()

	assert.Equal(t, 45*time.Second, s.sessionTimeout)
	assert.Equal(t, 30*time.Second, s.produceTimeout)
	assert.Equal(t, StartEarliest, s.startOffset)
	assert.Nil(t, s.sasl)
	assert.Nil(t, s.tls)
}

func TestSettings_ClientOpts(t *testing.T) {
	tests := []struct {
		name     string
		options  []Option
		wantOpts int
		wantErr  assert.ErrorAssertionFunc
	}{
		{
			name:     "brokers only",
			options:  []Option{WithBrokers("localhost:9092")},
			wantOpts: 1,
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.Nil(t, err)
			},
		},
		{
			name: "brokers, client id, sasl and tls",
			options: []Option{
				WithBrokers("a:9092", "b:9092"),
				WithClientID("billing"),
				WithSASLSCRAM("user", "pass", SCRAMSHA512),
				WithTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
			},
			wantOpts: 4,
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.Nil(t, err)
			},
		},
		{
			name:    "no brokers",
			options: []Option{WithClientID("billing")},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, ErrNoBrokers)
			},
		},
		{
			name: "unknown scram mechanism",
			options: []Option{
				WithBrokers("localhost:9092"),
				WithSASLSCRAM("user", "pass", SCRAMMechanism(99)),
			},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, ErrUnknownSCRAMMechanism)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := defaultSettings()
			for _, option := range tt.options {
				s = option(s)
			}

			opts, err := s.clientOpts()

			if !tt.wantErr(t, err) {
				return
			}

			if err == nil {
				assert.Len(t, opts, tt.wantOpts)
			}
		})
	}
}

func TestSettings_StartAt(t *testing.T) {
	earliest := defaultSettings()
	latest := WithStartOffset(StartLatest)(defaultSettings())

	assert.NotEqual(t, earliest.startAt().String(), latest.startAt().String())
}
