package validator

// IsEmpty reports whether value equals the zero value of its type. It treats
// "unset" and "explicitly zero" as the same thing, so a rule built on it cannot
// tell a missing number from a zero one.
func IsEmpty[T comparable](value T) bool {
	var empty T

	return empty == value
}

// ContainsKey reports whether m holds the key k.
func ContainsKey[M ~map[K]V, K comparable, V any](m M, k K) bool {
	_, ok := m[k]
	return ok
}
