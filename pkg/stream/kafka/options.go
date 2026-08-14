package kafka

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const (
	defaultSessionTimeout = 45 * time.Second

	// defaultProduceTimeout is how long a produced record may go
	// unacknowledged before delivery is reported as failed.
	//
	// The previous implementation used one second, against a librdkafka
	// default of five minutes, which turned any broker hiccup into a delivery
	// error. Thirty seconds rides out a leader election without hiding a
	// broker that is genuinely gone.
	defaultProduceTimeout = 30 * time.Second
)

// StartOffset is where a new consumer group begins reading.
type StartOffset int

const (
	// StartEarliest reads a topic from its oldest retained record.
	StartEarliest StartOffset = iota
	// StartLatest reads only records produced after the group joins.
	StartLatest
)

// SCRAMMechanism selects the hash used by SCRAM authentication.
type SCRAMMechanism int

const (
	// SCRAMSHA256 authenticates with SCRAM-SHA-256.
	SCRAMSHA256 SCRAMMechanism = iota
	// SCRAMSHA512 authenticates with SCRAM-SHA-512.
	SCRAMSHA512
)

// ErrUnknownSCRAMMechanism is returned when a SCRAMMechanism is not one of the
// constants declared by this package.
var ErrUnknownSCRAMMechanism = errors.New("kafka: unknown SCRAM mechanism")

type settings struct {
	brokers        []string
	clientID       string
	sasl           sasl.Mechanism
	saslErr        error
	tls            *tls.Config
	sessionTimeout time.Duration
	startOffset    StartOffset
	produceTimeout time.Duration
}

func defaultSettings() settings {
	return settings{
		sessionTimeout: defaultSessionTimeout,
		startOffset:    StartEarliest,
		produceTimeout: defaultProduceTimeout,
	}
}

// Option configures a Reader or a Writer. The same options serve both, so
// broker, authentication and TLS settings are written once for an application.
type Option func(s settings) settings

// WithBrokers sets the seed brokers, as "host:port" strings.
func WithBrokers(brokers ...string) Option {
	return func(s settings) settings {
		s.brokers = append(s.brokers, brokers...)

		return s
	}
}

// WithClientID sets the client id reported to the broker, which is what shows
// up in broker-side metrics and quotas.
func WithClientID(id string) Option {
	return func(s settings) settings {
		s.clientID = id

		return s
	}
}

// WithSASLPlain authenticates with SASL/PLAIN. Use it only over TLS - PLAIN
// sends the password unencrypted.
func WithSASLPlain(user, pass string) Option {
	return func(s settings) settings {
		s.sasl = plain.Plain(func(context.Context) (plain.Auth, error) {
			return plain.Auth{User: user, Pass: pass}, nil
		})

		return s
	}
}

// WithSASLSCRAM authenticates with SASL/SCRAM, which is what Confluent Cloud
// and MSK expect.
func WithSASLSCRAM(user, pass string, mechanism SCRAMMechanism) Option {
	return func(s settings) settings {
		auth := func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: user, Pass: pass}, nil
		}

		switch mechanism {
		case SCRAMSHA256:
			s.sasl = scram.Sha256(auth)
		case SCRAMSHA512:
			s.sasl = scram.Sha512(auth)
		default:
			// Options never return errors, so the failure is carried to the
			// constructor, which is where this package validates.
			s.saslErr = errors.Wrapf(ErrUnknownSCRAMMechanism, "%d", mechanism)
		}

		return s
	}
}

// WithTLS dials the brokers over TLS with the given configuration.
func WithTLS(cfg *tls.Config) Option {
	return func(s settings) settings {
		s.tls = cfg

		return s
	}
}

// WithSessionTimeout sets how long the broker waits for a heartbeat before
// evicting this member from its group. Defaults to 45s.
func WithSessionTimeout(d time.Duration) Option {
	return func(s settings) settings {
		s.sessionTimeout = d

		return s
	}
}

// WithStartOffset sets where a group with no committed offsets begins.
// Defaults to StartEarliest.
func WithStartOffset(o StartOffset) Option {
	return func(s settings) settings {
		s.startOffset = o

		return s
	}
}

// WithProduceTimeout sets how long a produced record may go unacknowledged
// before Produce reports a failure. Defaults to 30s.
//
// The driver rejects any non-zero timeout shorter than one second, so
// NewWriter returns an error rather than silently rounding it up.
func WithProduceTimeout(d time.Duration) Option {
	return func(s settings) settings {
		s.produceTimeout = d

		return s
	}
}

// clientOpts translates the settings into franz-go options shared by readers
// and writers.
func (s settings) clientOpts() ([]kgo.Opt, error) {
	if s.saslErr != nil {
		return nil, s.saslErr
	}

	if len(s.brokers) == 0 {
		return nil, ErrNoBrokers
	}

	opts := []kgo.Opt{kgo.SeedBrokers(s.brokers...)}

	if s.clientID != "" {
		opts = append(opts, kgo.ClientID(s.clientID))
	}

	if s.sasl != nil {
		opts = append(opts, kgo.SASL(s.sasl))
	}

	if s.tls != nil {
		opts = append(opts, kgo.DialTLSConfig(s.tls))
	}

	return opts, nil
}

func (s settings) startAt() kgo.Offset {
	if s.startOffset == StartLatest {
		return kgo.NewOffset().AtEnd()
	}

	return kgo.NewOffset().AtStart()
}
