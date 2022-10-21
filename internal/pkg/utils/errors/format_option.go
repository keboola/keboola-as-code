package errors

type FormatConfig struct {
	WithStack   bool // see FormatWithStack()
	AsSentences bool // see FormatAsSentences()
}

type FormatOption func(c *FormatConfig)

// FormatWithStack option includes stack trace in each error message.
func FormatWithStack() FormatOption {
	return func(c *FormatConfig) {
		c.WithStack = true
	}
}

// FormatAsSentences option converts standard Go error message to sentence.
// First letter is uppercase and dot is added to the end, if message doesn't end with a special character.
func FormatAsSentences() FormatOption {
	return func(c *FormatConfig) {
		c.AsSentences = true
	}
}
