package create

import (
	"context"
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/test/create"
)

type createTmplTestDialog struct {
	*dialog.Dialogs
	Flags
	out      createOp.Options
	template *template.Template
}

// AskCreateTemplateTestOptions - dialog for creating a template test.
func AskCreateTemplateTestOptions(ctx context.Context, d *dialog.Dialogs, template *template.Template, f Flags) (createOp.Options, []string, error) {
	dialog := &createTmplTestDialog{
		Dialogs:  d,
		template: template,
		Flags:    f,
	}
	return dialog.ask(ctx)
}

func (d *createTmplTestDialog) ask(ctx context.Context) (createOp.Options, []string, error) {
	// Instance name
	if v, err := d.askTestName(ctx); err != nil {
		return d.out, nil, err
	} else {
		d.out.TestName = v
	}

	// User inputs
	v, warnings, err := d.AskUseTemplateInputs(ctx, d.template.Inputs().ToExtended(), true, d.InputsFile)
	if err != nil {
		return d.out, nil, err
	} else {
		d.out.Inputs = v
	}

	return d.out, warnings, nil
}

func (d *createTmplTestDialog) askTestName(ctx context.Context) (string, error) {
	// Is flag set?
	if d.TestName.IsSet() {
		v := d.TestName.Value
		if len(v) > 0 {
			err := d.checkTestNameIsUnique(ctx, v)
			if err != nil {
				return "", err
			}
			return v, nil
		}
	}

	// Ask for instance name
	v, _ := d.Ask(&prompt.Question{
		Label:       "Test Name",
		Description: "Please enter a test name.",
		Validator: func(val any) error {
			str := strings.TrimSpace(val.(string))
			if len(str) == 0 {
				return errors.New(`test name is required`)
			}

			if !regexpcache.MustCompile(template.IDRegexp).MatchString(str) {
				return errors.Errorf(`invalid name "%s", please use only a-z, A-Z, 0-9, "-" characters`, str)
			}

			return d.checkTestNameIsUnique(ctx, str)
		},
	})
	if len(v) == 0 {
		return "", errors.New(`please specify test name`)
	}
	return v, nil
}

func (d *createTmplTestDialog) checkTestNameIsUnique(ctx context.Context, str string) error {
	_, err := d.template.Test(ctx, str)
	if err == nil {
		return errors.Errorf(`test "%s" already exists`, str)
	}
	if !strings.Contains(err.Error(), "not found in template") {
		return err
	}
	return nil
}
