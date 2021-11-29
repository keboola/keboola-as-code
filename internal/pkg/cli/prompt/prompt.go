package prompt

import (
	"errors"
)

type Confirm struct {
	Label       string
	Description string
	Default     bool
	Help        string
}

type Question struct {
	Label       string
	Description string
	Default     string
	Help        string
	Validator   func(val interface{}) error
	Hidden      bool
}

type Select struct {
	Label       string
	Description string
	Help        string
	Options     []string
	Default     string
	Validator   func(val interface{}) error
}

type SelectIndex struct {
	Label       string
	Description string
	Help        string
	Options     []string
	Default     int
	Validator   func(val interface{}) error
}

type MultiSelect struct {
	Label       string
	Description string
	Help        string
	Options     []string
	Default     []string
	Validator   func(val interface{}) error
}

type Prompt interface {
	IsInteractive() bool
	Printf(format string, a ...interface{})
	Confirm(c *Confirm) bool
	Ask(q *Question) (result string, ok bool)
	Select(s *Select) (value string, ok bool)
	SelectIndex(s *SelectIndex) (index int, ok bool)
	MultiSelect(s *MultiSelect) (result []string, ok bool)
	Multiline(q *Question) (result string, ok bool)
}

func ValueRequired(val interface{}) error {
	str := val.(string)
	if len(str) == 0 {
		return errors.New("value is required")
	}
	return nil
}
