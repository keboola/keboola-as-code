package dialog

import (
	"fmt"
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/test/create"
)

const testNameFlag = "test-name"

type createTmplTestDialog struct {
	*Dialogs
	inputs  input.StepsGroups
	options *options.Options
	out     createOp.Options
}

// AskCreateTemplateTestOptions - dialog for creating a template test.
func (p *Dialogs) AskCreateTemplateTestOptions(inputs template.StepsGroups, opts *options.Options) (createOp.Options, error) {
	dialog := &createTmplTestDialog{
		Dialogs: p,
		inputs:  inputs,
		options: opts,
	}
	return dialog.ask()
}

func (d *createTmplTestDialog) ask() (createOp.Options, error) {
	// Instance name
	if v, err := d.askTestName(); err != nil {
		return d.out, err
	} else {
		d.out.TestName = v
	}

	// User inputs
	if v, err := d.askUseTemplateInputs(d.inputs.ToExtended(), d.options); err != nil {
		return d.out, err
	} else {
		d.out.Inputs = v
	}

	return d.out, nil
}

func (d *createTmplTestDialog) askTestName() (string, error) {
	// Is flag set?
	if d.options.IsSet(testNameFlag) {
		v := d.options.GetString(testNameFlag)
		if len(v) > 0 {
			return v, nil
		}
	}

	// Ask for instance name
	v, _ := d.Dialogs.Ask(&prompt.Question{
		Label:       "Test Name",
		Description: "Please enter a test name.",
		Validator:   validateTestName,
	})
	if len(v) == 0 {
		return "", fmt.Errorf(`please specify test name`)
	}
	return v, nil
}

func validateTestName(val interface{}) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return fmt.Errorf(`test name is required`)
	}

	if !regexpcache.MustCompile(template.IdRegexp).MatchString(str) {
		return fmt.Errorf(`invalid name "%s", please use only a-z, A-Z, 0-9, "-" characters`, str)
	}

	return nil
}
