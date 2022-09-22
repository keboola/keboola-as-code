package test

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/testtemplateinputs"
)

// ReadInputValues read inputs and replaces env vars.
func ReadInputValues(ctx context.Context, tmpl *template.Template, test *template.Test) (template.InputsValues, error) {
	envInputsProvider, err := testtemplateinputs.CreateTestInputsEnvProvider(ctx)
	if err != nil {
		return nil, err
	}
	inputsFile, err := test.Inputs(envInputsProvider, testhelper.ReplaceEnvsStringWithSeparatorE, "##")
	if err != nil {
		return nil, err
	}

	inputValues := make(template.InputsValues, 0)
	err = tmpl.Inputs().ToExtended().VisitInputs(func(group *input.StepsGroupExt, step *input.StepExt, inputDef *input.Input) error {
		var inputValue template.InputValue
		if v, found := inputsFile[inputDef.Id]; found {
			inputValue, err = template.ParseInputValue(v, inputDef, true)
			if err != nil {
				return utils.PrefixError(err.Error(), fmt.Errorf("please fix the value in the inputs JSON file"))
			}
		} else {
			inputValue, err = template.ParseInputValue(inputDef.DefaultOrEmpty(), inputDef, true)
			if err != nil {
				return utils.PrefixError(err.Error(), fmt.Errorf("please define value in the inputs JSON file"))
			}
		}
		inputValues = append(inputValues, inputValue)
		return nil
	})
	return inputValues, err
}
