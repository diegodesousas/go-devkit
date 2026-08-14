package kafka

import "github.com/pkg/errors"

// Errors returned when a client cannot be built.
var (
	// ErrNoBrokers means WithBrokers was never called, or was called with an
	// empty list. There is no default broker: guessing localhost would turn a
	// misconfiguration into a connection that silently goes nowhere.
	ErrNoBrokers = errors.New("kafka: at least one broker is required")

	// ErrNoTopics means NewReader was called with an empty topic list.
	ErrNoTopics = errors.New("kafka: at least one topic is required")

	// ErrNoGroupID means NewReader was called with an empty group id.
	ErrNoGroupID = errors.New("kafka: a consumer group id is required")
)
