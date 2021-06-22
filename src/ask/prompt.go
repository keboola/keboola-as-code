package ask

import (
	"errors"
	"fmt"
	"github.com/AlecAivazis/survey/v2"
	"github.com/AlecAivazis/survey/v2/terminal"
	"net/url"
	"os"
)

type Prompt struct {
	Interactive bool // is terminal interactive?, can we ask questions?
	stdin       terminal.FileReader
	stdout      terminal.FileWriter
	stderr      terminal.FileWriter
}

type Question struct {
	Label       string
	Description string
	Help        string
	Validator   func(val interface{}) error
	Hidden      bool
}

func NewPrompt(stdin terminal.FileReader, stdout terminal.FileWriter, stderr terminal.FileWriter) *Prompt {
	return &Prompt{
		Interactive: isInteractiveTerminal(),
		stdin:       stdin,
		stdout:      stdout,
		stderr:      stderr,
	}
}

func (p *Prompt) Ask(q *Question) (result string, ok bool) {
	var opts []survey.AskOpt
	var err error

	// Ask only in the interactive terminal
	if !p.Interactive {
		return "", false
	}

	// Print description
	if len(q.Description) > 0 {
		if _, err = fmt.Fprintf(p.stdout, "%s\n", q.Description); err != nil {
			panic(err)
		}
	}

	// Settings
	opts = append(opts, survey.WithStdio(p.stdin, p.stdout, p.stderr))
	opts = append(opts, survey.WithShowCursor(true))

	// Validator
	if q.Validator != nil {
		opts = append(opts, survey.WithValidator(q.Validator))
	}

	// Ask
	if q.Hidden {
		err = survey.AskOne(&survey.Password{Message: q.Label, Help: q.Help}, &result, opts...)
	} else {
		err = survey.AskOne(&survey.Input{Message: q.Label, Help: q.Help}, &result, opts...)
	}

	// Handle error
	if err == terminal.InterruptErr {
		// Ctrl+c -> append new line after prompt AND exit program
		_, _ = p.stdout.Write([]byte("\n"))
		if v, ok := p.stdout.(*os.File); ok {
			_ = v.Sync()
		}
		os.Exit(1)
	} else if err != nil {
		return "", false
	}

	return result, true
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
