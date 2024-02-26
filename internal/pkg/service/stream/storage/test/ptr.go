package test

// Ptr returns valued pointer, it is used to get a pointer within an inline definition.
func Ptr[T any](v T) *T {
	return &v
}
