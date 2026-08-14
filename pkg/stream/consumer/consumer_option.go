package consumer

import (
    "github.com/diegodesousas/go-devkit/pkg/gen"
    "github.com/diegodesousas/go-devkit/pkg/log"
)

type settings struct {
    logger          log.Logger
    stringGenerator gen.StringGenerator
}

// Option configures a Consumer built by New.
type Option func(s settings) settings

// WithLogger sets the base logger. The Consumer derives from it per message,
// adding the group, topic, partition, offset and a trace id. Defaults to
// log.New().
func WithLogger(logger log.Logger) Option {
    return func(s settings) settings {
        s.logger = logger

        return s
    }
}

// WithStringGenerator sets the source of per-message trace ids. Defaults to
// gen.UUIDGenerator; inject gen.SequenceGenerator in tests for deterministic
// output.
func WithStringGenerator(generator gen.StringGenerator) Option {
    return func(s settings) settings {
        s.stringGenerator = generator

        return s
    }
}
