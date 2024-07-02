package interactive

import (
	"fmt"
	"io"
	"os"

	"github.com/AlecAivazis/survey/v2"
	"github.com/AlecAivazis/survey/v2/terminal"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type Prompt struct {
	stdin  terminal.FileReader
	stdout terminal.FileWriter
	stderr io.Writer
	editor string // the editor is started when Editor() is called, if empty, the default is system editor is used
}

//nolint:gochecknoinits
func init() {
	// Workaround for bug in 3rd party lib
	// https://github.com/AlecAivazis/survey/issues/336
	survey.MultilineQuestionTemplate += `{{"\n"}}`
}

func New(stdin terminal.FileReader, stdout terminal.FileWriter, stderr io.Writer) *Prompt {
	return &Prompt{
		stdin:  stdin,
		stdout: stdout,
		stderr: stderr,
	}
}

func (p *Prompt) SetEditor(editor string) {
	p.editor = editor
}

func (p *Prompt) IsInteractive() bool {
	return true
}

func (p *Prompt) Printf(format string, a ...any) {
	// The error can occur mainly in tests, if stdout of the virtual terminal is closed on test failure.
	_, _ = fmt.Fprintf(p.stdout, format, a...)
}

func (p *Prompt) Confirm(c *prompt.Confirm) bool {
	// Print description
	if len(c.Description) > 0 {
		p.Printf("\n%s\n", c.Description)
	} else {
		p.Printf("\n")
	}

	result := c.Default
	opts := p.getOpts()
	err := survey.AskOne(&survey.Confirm{Message: formatLabel(c.Label), Help: c.Help, Default: c.Default}, &result, opts...)
	_ = p.handleError(err)
	p.Printf("\n")
	return result
}

func (p *Prompt) Ask(q *prompt.Question) (result string, ok bool) {
	var err error

	// Print description
	if len(q.Description) > 0 {
		p.Printf("\n%s\n", q.Description)
	} else {
		p.Printf("\n")
	}

	if q.Hidden && len(q.Default) > 0 {
		p.Printf("Leave blank for default value.\n")
	}

	// Validator
	opts := p.getOpts()
	if q.Validator != nil {
		if q.Hidden && q.Default != "" {
			original := q.Validator
			q.Validator = func(val any) error {
				if val == "" {
					val = q.Default
				}
				return original(val)
			}
		}
		opts = append(opts, survey.WithValidator(q.Validator))
	}

	// Ask
	if q.Hidden {
		err = survey.AskOne(&survey.Password{Message: formatLabel(q.Label), Help: q.Help}, &result, opts...)
		if result == "" && q.Default != "" {
			result = q.Default
		}
	} else {
		err = survey.AskOne(&survey.Input{Message: formatLabel(q.Label), Default: q.Default, Help: q.Help}, &result, opts...)
	}

	p.Printf("\n")
	return result, p.handleError(err)
}

func (p *Prompt) Select(s *prompt.Select) (value string, ok bool) {
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

	question := &survey.Select{Message: formatLabel(s.Label), Help: s.Help, Options: s.Options}
	if s.UseDefault {
		question.Default = s.Default
	}

	err := survey.AskOne(question, &value, opts...)
	p.Printf("\n")
	return value, p.handleError(err)
}

func (p *Prompt) SelectIndex(s *prompt.SelectIndex) (index int, ok bool) {
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

	question := &survey.Select{Message: formatLabel(s.Label), Help: s.Help, Options: s.Options}
	if s.UseDefault {
		question.Default = s.Options[s.Default]
	}

	err := survey.AskOne(question, &index, opts...)
	p.Printf("\n")
	return index, p.handleError(err)
}

func (p *Prompt) MultiSelect(s *prompt.MultiSelect) (result []string, ok bool) {
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

	err := survey.AskOne(&survey.MultiSelect{Message: formatLabel(s.Label), Help: s.Help, Options: s.Options, Default: s.Default}, &result, opts...)
	p.Printf("\n")
	return result, p.handleError(err)
}

func (p *Prompt) MultiSelectIndex(s *prompt.MultiSelectIndex) (result []int, ok bool) {
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

	err := survey.AskOne(&survey.MultiSelect{Message: formatLabel(s.Label), Help: s.Help, Options: s.Options, Default: s.Default}, &result, opts...)
	p.Printf("\n")
	return result, p.handleError(err)
}

func (p *Prompt) Multiline(q *prompt.Question) (result string, ok bool) {
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

	err := survey.AskOne(&survey.Multiline{Message: formatLabel(q.Label), Default: q.Default, Help: q.Help}, &result, opts...)
	p.Printf("\n")
	return result, p.handleError(err)
}

func (p *Prompt) Editor(fileExt string, q *prompt.Question) (result string, ok bool) {
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

	fileName := `kbc-editor-*.` + fileExt
	editor := &survey.Editor{Message: formatLabel(q.Label), Default: q.Default, Help: q.Help, HideDefault: true, AppendDefault: true, Editor: p.editor, FileName: fileName}
	err := survey.AskOne(editor, &result, opts...)
	p.Printf("\n")
	return result, p.handleError(err)
}

func (p *Prompt) getOpts() []survey.AskOpt {
	var opts []survey.AskOpt
	opts = append(opts, survey.WithStdio(p.stdin, p.stdout, p.stderr))
	opts = append(opts, survey.WithShowCursor(true))
	opts = append(opts, survey.WithFilter(func(filter string, value string, index int) (include bool) {
		return strhelper.MatchWords(value, filter)
	}))
	return opts
}

func (p *Prompt) handleError(err error) (ok bool) {
	if err == nil {
		return true
	} else if errors.Is(err, terminal.InterruptErr) {
		// Ctrl+c -> append new line after prompt AND exit program
		_, _ = p.stdout.Write([]byte("\n"))
		if v, ok := p.stdout.(*os.File); ok {
			_ = v.Sync()
		}
		os.Exit(1)
	}

	return false
}

func formatLabel(label string) string {
	// Add ":" if string ends with alphanumeric char
	if regexpcache.MustCompile(`[a-zA-Z0-9]$`).MatchString(label) {
		label += `:`
	}
	return label
}
