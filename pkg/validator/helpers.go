package validator

func IsEmpty[T comparable](value T) bool {
	var empty T

	return empty == value
}

func ContainsKey[M ~map[K]V, K comparable, V any](m M, k K) bool {
	_, ok := m[k]
	return ok
}
