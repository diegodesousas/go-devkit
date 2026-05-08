package consumer

import (
    "github.com/diegodesousas/go-devkit/pkg/gen"
    "github.com/diegodesousas/go-devkit/pkg/log"
)

type settings struct {
    logger          log.Logger
    stringGenerator gen.StringGenerator
}

type Option func(s settings) settings

func WithLogger(logger log.Logger) Option {
    return func(s settings) settings {
        s.logger = logger

        return s
    }
}

func WithStringGenerator(generator gen.StringGenerator) Option {
    return func(s settings) settings {
        s.stringGenerator = generator

        return s
    }
}
