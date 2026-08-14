package mapper

import "errors"

// ErrValueNotFound is returned by Get when the key was never set.
var ErrValueNotFound = errors.New("err value not found")

// Mapper is a registry of values of type T keyed by K, where K is any type
// whose underlying type is string.
//
// Set returns the Mapper so calls can be chained, but the returned value shares
// the backing map with the receiver - it is not a copy. A Mapper is not safe for
// concurrent use.
type Mapper[K ~string, T any] interface {
	Set(key K, value T) Mapper[K, T]
	Get(k K) (value T, err error)
}

// New returns an empty Mapper.
func New[K ~string, T any]() Mapper[K, T] {
	return mapper[K, T]{
		list: make(map[K]T),
	}
}

type mapper[K ~string, T any] struct {
	list map[K]T
}

func (m mapper[K, T]) Set(key K, value T) Mapper[K, T] {
	m.list[key] = value

	return m
}

func (m mapper[K, T]) Get(key K) (value T, err error) {
	value, ok := m.list[key]
	if !ok {
		return value, ErrValueNotFound
	}

	return value, nil
}
