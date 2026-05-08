package log

import (
	"context"
	"io"
	"os"

	"github.com/sirupsen/logrus"
)

const (
	// loggerKey  is the key used to store a logger in a context.Context
	loggerKey key = "logger"
)

type (
	// key is a type used to store a logger in a context.Context
	key string
)

type logger struct {
	entry *logrus.Entry
}

type settings struct {
	level    logrus.Level
	format   logrus.Formatter
	output   io.Writer
	exitFunc func(code int)
}

type Option func(*settings)

func New(options ...Option) Logger {
	settings := settings{
		level:    logrus.InfoLevel,
		format:   &logrus.TextFormatter{},
		output:   os.Stderr,
		exitFunc: os.Exit,
	}

	for _, o := range options {
		o(&settings)
	}

	l := logrus.New()
	l.SetLevel(settings.level)
	l.SetFormatter(settings.format)
	l.SetOutput(settings.output)
	l.ExitFunc = settings.exitFunc

	return &logger{
		entry: logrus.NewEntry(l),
	}
}

func WithLevel(level Level) Option {
	return func(o *settings) {
		switch level {
		case InfoLevel:
			o.level = logrus.InfoLevel
		case ErrorLevel:
			o.level = logrus.ErrorLevel
		case WarnLevel:
			o.level = logrus.WarnLevel
		case DebugLevel:
			o.level = logrus.DebugLevel
		}
	}
}

func WithJsonFormat() Option {
	return func(o *settings) {
		o.format = &logrus.JSONFormatter{}
	}
}

func WithOutput(output io.Writer) Option {
	return func(o *settings) {
		o.output = output
	}
}

func WithExitFunc(exitFunc func(code int)) Option {
	return func(o *settings) {
		o.exitFunc = exitFunc
	}
}

func (l *logger) WithFields(fields ...Field) Logger {
	cpLogger := logger{}
	cpLogger.entry = l.entry.WithFields(toLogFields(fields...))

	return &cpLogger
}

func (l *logger) Info(msg string, fields ...Field) {
	l.entry.
		WithFields(toLogFields(fields...)).
		Info(msg)
}

func (l *logger) Error(msg string, fields ...Field) {
	l.entry.
		WithFields(toLogFields(fields...)).
		Error(msg)
}

func (l *logger) Debug(msg string, fields ...Field) {
	l.entry.
		WithFields(toLogFields(fields...)).
		Debug(msg)
}

func (l *logger) Warn(msg string, fields ...Field) {
	l.entry.
		WithFields(toLogFields(fields...)).
		Warn(msg)
}

func (l *logger) Fatal(msg string, fields ...Field) {
	l.entry.
		WithFields(toLogFields(fields...)).
		Fatal(msg)
}

func toLogFields(fields ...Field) logrus.Fields {
	newFields := logrus.Fields{}

	for _, field := range fields {
		newFields[field.Key] = field.Value
	}

	return newFields
}

func WithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

func FromContext(ctx context.Context) Logger {
	logger, ok := ctx.Value(loggerKey).(Logger)

	if !ok {
		return New()
	}

	return logger
}

func WithFields(ctx context.Context, fields ...Field) context.Context {
	return WithLogger(
		ctx,
		FromContext(ctx).WithFields(fields...),
	)
}
