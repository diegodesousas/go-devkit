// Package gen provides string generators, used across the devkit wherever an
// identifier has to be produced.
//
// A StringGenerator is a function, so it is injected rather than called
// directly - which is what makes the identifier deterministic in tests:
//
//	middleware := httpserver.TraceID(gen.UUIDGenerator())  // production
//	middleware := httpserver.TraceID(gen.SequenceGenerator()) // tests
//
// UUIDGenerator produces random UUIDv4 strings. ULIDGenerator produces ULIDs,
// which sort lexicographically by creation time - useful when the identifier
// doubles as a sort key. SequenceGenerator counts from one; it is safe to call
// from multiple goroutines, but the sequence restarts with each generator, so it
// is meant for tests rather than for identifiers that must be unique across
// processes.
package gen
