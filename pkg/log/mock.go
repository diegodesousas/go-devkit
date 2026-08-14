package log

import (
	"github.com/stretchr/testify/mock"
)

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
