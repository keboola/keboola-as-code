package prompt

import (
	"errors"
	"strings"

	"github.com/AlecAivazis/survey/v2/core"
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
	UseDefault  bool
	Validator   func(val interface{}) error
}

type SelectIndex struct {
	Label       string
	Description string
	Help        string
	Options     []string
	Default     int
	UseDefault  bool
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

type MultiSelectIndex struct {
	Label       string
	Description string
	Help        string
	Options     []string
	Default     []int
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
	MultiSelectIndex(s *MultiSelectIndex) (result []int, ok bool)
	Multiline(q *Question) (result string, ok bool)
	// Editor allows you to edit text using the editor specified via env EDITOR.
	// fileExt is the extension of the temporary file, for syntax highlighting.
	Editor(fileExt string, q *Question) (result string, ok bool)
}

func ValueRequired(val interface{}) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return errors.New("value is required")
	}
	return nil
}

func AtLeastOneRequired(val interface{}) error {
	items := val.([]core.OptionAnswer)
	if len(items) == 0 {
		return errors.New("at least one value is required")
	}
	return nil
}
