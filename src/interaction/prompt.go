package interaction

import (
	"errors"
	"fmt"
	"net/url"
	"os"

	"github.com/AlecAivazis/survey/v2"
	"github.com/AlecAivazis/survey/v2/terminal"
)

type Prompt struct {
	Interactive bool // is terminal interactive?, can we ask questions?
	stdin       terminal.FileReader
	stdout      terminal.FileWriter
	stderr      terminal.FileWriter
}

type Confirm struct {
	Label       string
	Description string
	Default     bool
	Help        string
}

type Question struct {
	Label       string
	Description string
	Help        string
	Validator   func(val interface{}) error
	Hidden      bool
}

type Select struct {
	Label       string
	Description string
	Help        string
	Options     []string
	Default     interface{}
	Validator   func(val interface{}) error
}

func NewPrompt(stdin terminal.FileReader, stdout terminal.FileWriter, stderr terminal.FileWriter) *Prompt {
	return &Prompt{
		Interactive: isInteractiveTerminal(),
		stdin:       stdin,
		stdout:      stdout,
		stderr:      stderr,
	}
}

// nolint: gochecknoinits
func init() {
	// Workaround for bug in 3rd party lib
	// https://github.com/AlecAivazis/survey/issues/336
	survey.MultilineQuestionTemplate += `{{"\n"}}`
}

func (p *Prompt) Confirm(c *Confirm) bool {
	// Ask only in the interactive terminal
	if !p.Interactive {
		return c.Default
	}

	// Print description
	if len(c.Description) > 0 {
		p.Printf("\n%s\n", c.Description)
	} else {
		p.Printf("\n")
	}

	result := c.Default
	opts := p.getOpts()
	err := survey.AskOne(&survey.Confirm{Message: c.Label, Help: c.Help, Default: c.Default}, &result, opts...)
	_ = p.handleError(err)
	return result
}

func (p *Prompt) Ask(q *Question) (result string, ok bool) {
	var err error

	// Ask only in the interactive terminal
	if !p.Interactive {
		return "", false
	}

	// Print description
	if len(q.Description) > 0 {
		p.Printf("\n%s\n", q.Description)
	} else {
		p.Printf("\n")
	}

	// Validator
	opts := p.getOpts()
	if q.Validator != nil {
		opts = append(opts, survey.WithValidator(q.Validator))
	}

	// Ask
	if q.Hidden {
		err = survey.AskOne(&survey.Password{Message: q.Label, Help: q.Help}, &result, opts...)
	} else {
		err = survey.AskOne(&survey.Input{Message: q.Label, Help: q.Help}, &result, opts...)
	}

	return result, p.handleError(err)
}

func (p *Prompt) Select(s *Select) (result string, ok bool) {
	// Ask only in the interactive terminal
	if !p.Interactive {
		return "", false
	}

	// Print description
	if len(s.Description) > 0 {
		p.Printf("\n%s\n", s.Description)
	} else {
		p.Printf("\n")
	}

	// Validator
	opts := p.getOpts()
	if s.Validator != nil {
		opts = append(opts, survey.WithValidator(s.Validator))
	}

	err := survey.AskOne(&survey.Select{Message: s.Label, Help: s.Help, Options: s.Options, Default: s.Default}, &result, opts...)
	return result, p.handleError(err)
}

func (p *Prompt) MultiSelect(s *Select) (result []string, ok bool) {
	// Ask only in the interactive terminal
	if !p.Interactive {
		return nil, false
	}

	// Print description
	if len(s.Description) > 0 {
		p.Printf("\n%s\n", s.Description)
	} else {
		p.Printf("\n")
	}

	// Validator
	opts := p.getOpts()
	if s.Validator != nil {
		opts = append(opts, survey.WithValidator(s.Validator))
	}

	err := survey.AskOne(&survey.MultiSelect{Message: s.Label, Help: s.Help, Options: s.Options, Default: s.Default}, &result, opts...)
	return result, p.handleError(err)
}

func (p *Prompt) Multiline(q *Question) (result string, ok bool) {
	// Ask only in the interactive terminal
	if !p.Interactive {
		return "", false
	}

	// Print description
	if len(q.Description) > 0 {
		p.Printf("\n%s\n", q.Description)
	} else {
		p.Printf("\n")
	}

	// Validator
	opts := p.getOpts()
	if q.Validator != nil {
		opts = append(opts, survey.WithValidator(q.Validator))
	}

	err := survey.AskOne(&survey.Multiline{Message: q.Label, Help: q.Help}, &result, opts...)
	return result, p.handleError(err)
}

func (p *Prompt) getOpts() []survey.AskOpt {
	var opts []survey.AskOpt
	opts = append(opts, survey.WithStdio(p.stdin, p.stdout, p.stderr))
	opts = append(opts, survey.WithShowCursor(true))
	return opts
}

func (p *Prompt) Printf(format string, a ...interface{}) {
	if _, err := fmt.Fprintf(p.stdout, format, a...); err != nil {
		panic(err)
	}
}

func (p *Prompt) handleError(err error) (ok bool) {
	if err == nil {
		return true
	} else if err == terminal.InterruptErr {
		// Ctrl+c -> append new line after prompt AND exit program
		_, _ = p.stdout.Write([]byte("\n"))
		if v, ok := p.stdout.(*os.File); ok {
			_ = v.Sync()
		}
		os.Exit(1)
	}

	return false
}

func ApiHostValidator(val interface{}) error {
	str := val.(string)
	if len(str) == 0 {
		return errors.New("value is required")
	} else if _, err := url.Parse(str); err != nil {
		return errors.New("invalid host")
	}
	return nil
}

func ValueRequired(val interface{}) error {
	str := val.(string)
	if len(str) == 0 {
		return errors.New("value is required")
	}
	return nil
}

func isInteractiveTerminal() bool {
	if fileInfo, _ := os.Stdin.Stat(); (fileInfo.Mode() & os.ModeCharDevice) == 0 {
		return false
	}

	if fileInfo, _ := os.Stdout.Stat(); (fileInfo.Mode() & os.ModeCharDevice) == 0 {
		return false
	}

	return true
}
