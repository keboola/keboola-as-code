package manifest

import "fmt"

type Error struct {
	errors []error
}

func (e *Error) Len() int {
	return len(e.errors)
}

func (e *Error) Add(err error) {
	e.errors = append(e.errors, err)
}

func (e *Error) Error() string {
	if len(e.errors) == 0 {
		return ""
	}

	msg := "Manifest is not valid:\n"
	for _, err := range e.errors {
		msg += fmt.Sprintf("- %s\n", err)
	}
	return msg
}
