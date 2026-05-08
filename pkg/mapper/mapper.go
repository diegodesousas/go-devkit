package mapper

import "errors"

var ErrValueNotFound = errors.New("err value not found")

type Mapper[K ~string, T any] interface {
	Set(key K, value T) Mapper[K, T]
	Get(k K) (value T, err error)
}

func NewMapper[K ~string, T any]() Mapper[K, T] {
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
