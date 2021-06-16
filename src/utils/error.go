package utils

import (
	"fmt"
	"strings"
)

type Error struct {
	errors []error
}

func (e *Error) Len() int {
	return len(e.errors)
}

func (e *Error) Add(err error) {
	e.errors = append(e.errors, err)
}

func (e *Error) Errors() []error {
	return e.errors
}

func (e *Error) Error() string {
	if len(e.errors) == 0 {
		return ""
	}

	if len(e.errors) == 0 {
		return ""
	} else if len(e.errors) == 1 {
		return e.errors[0].Error()
	}

	var msg []string
	for _, err := range e.errors {
		msg = append(msg, fmt.Sprintf("- %s", err))
	}

	return "\n" + strings.Join(msg, "\n")
}
