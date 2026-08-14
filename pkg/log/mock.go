package log

import (
	"github.com/stretchr/testify/mock"
)

// LoggerMock is a testify mock implementing Logger.
//
// The leveled methods spread their fields as separate arguments, so an
// expectation lists them one by one:
//
//	m.On("Info", "started", log.NewField("port", 8080))
//
// WithFields is the exception: it takes the fields as a single slice argument.
type LoggerMock struct {
	mock.Mock
}

func (l *LoggerMock) Info(msg string, fields ...Field) {
	var params []any
	params = append(params, msg)

	for _, field := range fields {
		params = append(params, field)
	}

	l.Called(params...)
}

func (l *LoggerMock) Error(msg string, fields ...Field) {
	var params []any
	params = append(params, msg)

	for _, field := range fields {
		params = append(params, field)
	}

	l.Called(params...)
}

func (l *LoggerMock) Debug(msg string, fields ...Field) {
	var params []any
	params = append(params, msg)

	for _, field := range fields {
		params = append(params, field)
	}

	l.Called(params...)
}

func (l *LoggerMock) Warn(msg string, fields ...Field) {
	var params []any
	params = append(params, msg)

	for _, field := range fields {
		params = append(params, field)
	}

	l.Called(params...)
}

func (l *LoggerMock) Fatal(msg string, fields ...Field) {
	var params []any
	params = append(params, msg)

	for _, field := range fields {
		params = append(params, field)
	}

	l.Called(params...)
}

func (l *LoggerMock) WithFields(fields ...Field) Logger {
	arguments := l.Called(fields)

	return arguments.Get(0).(Logger)
}
