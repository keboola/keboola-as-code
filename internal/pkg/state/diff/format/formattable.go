package format

// Formattable is a type that should be specially formatted in the diff output.
type Formattable interface {
	Format(f PathFormatter) string
}
