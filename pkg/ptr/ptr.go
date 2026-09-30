package ptr

func Zero[T any]() T {
	var zero T
	return zero
}
