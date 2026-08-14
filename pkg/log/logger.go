package log

// Level is the minimum severity a logger emits, selected with WithLevel.
//
// The values are identifiers, not an ordering: they do not increase with
// severity, so comparing two Levels says nothing about which is more verbose.
type Level uint

// The levels a logger can be configured with.
const (
	InfoLevel Level = iota
	ErrorLevel
	WarnLevel
	DebugLevel
)

// Field is one structured key/value pair attached to a log entry. Build one
// with NewField.
type Field struct {
	Key   string
	Value any
}

// Logger writes leveled entries with structured fields.
//
// The interface is intentionally small so the backend behind it can be replaced
// without changing callers. New returns the logrus-backed implementation, and
// LoggerMock implements it for tests.
//
// Fatal writes the entry and then terminates the process.
type Logger interface {
	Info(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Debug(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Fatal(msg string, fields ...Field)
	WithFields(fields ...Field) Logger
}
