// Package mapper is a small generic registry keyed by a string-like type.
//
// It exists to look values up by a named key type rather than a bare string, so
// the compiler rejects a lookup with the wrong kind of key:
//
//	type Currency string
//
//	rates := mapper.New[Currency, float64]()
//	rates.Set("BRL", 1).Set("USD", 5.4)
//
//	rate, err := rates.Get("BRL")
//
// A key that was never set yields ErrValueNotFound.
//
// Set chains, but it does not copy: the returned Mapper shares the backing map
// with the receiver, so both see every write. Treat the return value as a
// convenience for chaining, not as a new value.
//
// A Mapper is not safe for concurrent use. Populate it during start-up and read
// it afterwards, or guard it yourself.
package mapper
