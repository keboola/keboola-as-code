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

func (e *Error) Error() string {
	if len(e.errors) == 0 {
		return ""
	}

	var msg []string
	for _, err := range e.errors {
		msg = append(msg, fmt.Sprintf("- %s", err))
	}

	if len(msg) == 0 {
		return ""
	}
	return strings.Join(msg, "\n") + "\n"
}
