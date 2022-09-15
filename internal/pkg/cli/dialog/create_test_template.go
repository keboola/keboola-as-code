package dialog

import (
	"fmt"
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/test/create"
)

const testNameFlag = "test-name"

type createTmplTestDialog struct {
	*Dialogs
	options  *options.Options
	out      createOp.Options
	template *template.Template
}

// AskCreateTemplateTestOptions - dialog for creating a template test.
func (p *Dialogs) AskCreateTemplateTestOptions(template *template.Template, opts *options.Options) (createOp.Options, []string, error) {
	dialog := &createTmplTestDialog{
		Dialogs:  p,
		template: template,
		options:  opts,
	}
	return dialog.ask()
}

func (d *createTmplTestDialog) ask() (createOp.Options, []string, error) {
	// Instance name
	if v, err := d.askTestName(); err != nil {
		return d.out, nil, err
	} else {
		d.out.TestName = v
	}

	// User inputs
	v, warnings, err := d.askUseTemplateInputs(d.template.Inputs().ToExtended(), d.options, true)
	if err != nil {
		return d.out, nil, err
	} else {
		d.out.Inputs = v
	}

	return d.out, warnings, nil
}

func (d *createTmplTestDialog) askTestName() (string, error) {
	// Is flag set?
	if d.options.IsSet(testNameFlag) {
		v := d.options.GetString(testNameFlag)
		if len(v) > 0 {
			err := d.checkTestNameIsUnique(v)
			if err != nil {
				return "", err
			}
			return v, nil
		}
	}

	// Ask for instance name
	v, _ := d.Dialogs.Ask(&prompt.Question{
		Label:       "Test Name",
		Description: "Please enter a test name.",
		Validator: func(val interface{}) error {
			str := strings.TrimSpace(val.(string))
			if len(str) == 0 {
				return fmt.Errorf(`test name is required`)
			}

			if !regexpcache.MustCompile(template.IdRegexp).MatchString(str) {
				return fmt.Errorf(`invalid name "%s", please use only a-z, A-Z, 0-9, "-" characters`, str)
			}

			return d.checkTestNameIsUnique(str)
		},
	})
	if len(v) == 0 {
		return "", fmt.Errorf(`please specify test name`)
	}
	return v, nil
}

func (d *createTmplTestDialog) checkTestNameIsUnique(str string) error {
	_, err := d.template.Test(str)
	if err == nil {
		return fmt.Errorf(`test "%s" already exists`, str)
	}
	if !strings.Contains(err.Error(), "not found in template") {
		return err
	}
	return nil
}
