package utils

import (
	"fmt"
	"regexp"
	"strings"
)

type Error struct {
	prefix string
	errors []string
}

func WrapError(prefix string, err error) *Error {
	e := &Error{}
	e.SetPrefix(prefix + ":")
	e.Add(err)
	return e
}

func (e *Error) Len() int {
	return len(e.errors)
}

func (e *Error) SetPrefix(prefix string) {
	e.prefix = prefix
}

func (e *Error) Add(err error) {
	if v, ok := err.(*Error); ok {
		for _, item := range v.Errors() {
			e.doAdd(item)
		}
	} else {
		e.doAdd(err.Error())
	}
}

func (e *Error) AddRaw(err string) {
	e.errors = append(e.errors, err)
}

func (e *Error) AddSubError(prefix string, err error) {
	// Prefix each line with "-"
	str := regexp.MustCompile(`((^|\n)\s*-*)`).ReplaceAllString(err.Error(), "${2}\t-")
	e.doAdd(fmt.Sprintf("%s:\n%s", prefix, str))
}

func (e *Error) Errors() []string {
	return e.errors
}

func (e *Error) Error() string {
	if len(e.errors) == 0 {
		return ""
	}

	msg := strings.Join(e.errors, "\n")
	if e.prefix != "" {
		return e.prefix + "\n" + msg
	}

	return msg
}

func (e *Error) doAdd(err string) {
	err = strings.TrimLeft(err, "- ")
	err = fmt.Sprintf("- %s", err)
	e.errors = append(e.errors, err)
}
