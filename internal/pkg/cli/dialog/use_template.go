package dialog

import (
	"context"
	"fmt"
	"strconv"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type contextKey string

func (p *Dialogs) AskUseTemplateOptions(inputs input.Inputs) (results map[string]interface{}, err error) {
	results = make(map[string]interface{})
	ctx := context.Background()
	errors := utils.NewMultiError()
	for _, i := range inputs {
		if !i.Available(results) {
			continue
		}
		switch i.Kind {
		case input.KindInput, input.KindPassword, input.KindTextarea:
			question := &prompt.Question{
				Label:       i.Name,
				Description: i.Description,
				Validator: func(raw interface{}) error {
					value, err := convertType(raw, i.Type)
					if err != nil {
						return err
					}
					return i.ValidateUserInput(value, ctx)
				},
				Hidden: i.Kind == input.KindPassword,
			}
			if i.Default != nil {
				question.Default = i.Default.(string)
			}
			value, _ := p.Ask(question)
			ctx = context.WithValue(ctx, contextKey(i.Id), value)
			results[i.Id], _ = convertType(value, i.Type)
		case input.KindConfirm:
			confirm := &prompt.Confirm{
				Label:       i.Name,
				Description: i.Description,
			}
			if i.Default != nil {
				confirm.Default = i.Default.(bool)
			}
			value := p.Confirm(confirm)
			ctx = context.WithValue(ctx, contextKey(i.Id), value)
			results[i.Id] = value
		case input.KindSelect:
			selectPrompt := &prompt.Select{
				Label:       i.Name,
				Description: i.Description,
				Options:     i.Options,
				UseDefault:  true,
				Validator: func(val interface{}) error {
					return i.ValidateUserInput(val, ctx)
				},
			}
			if i.Default != nil {
				selectPrompt.Default = i.Default.(string)
			}
			value, _ := p.Select(selectPrompt)
			ctx = context.WithValue(ctx, contextKey(i.Id), value)
			results[i.Id] = value
		case input.KindMultiSelect:
			multiSelect := &prompt.MultiSelect{
				Label:       i.Name,
				Description: i.Description,
				Options:     i.Options,
				Validator: func(val interface{}) error {
					return i.ValidateUserInput(val, ctx)
				},
			}
			if i.Default != nil {
				multiSelect.Default = i.Default.([]string)
			}
			value, _ := p.MultiSelect(multiSelect)
			ctx = context.WithValue(ctx, contextKey(i.Id), value)
			results[i.Id] = value
		}
	}

	return results, errors.ErrorOrNil()
}

func convertType(inputValue interface{}, inputType string) (interface{}, error) {
	switch inputType {
	case `int`:
		if v, err := strconv.Atoi(inputValue.(string)); err == nil {
			return v, nil
		} else {
			return nil, fmt.Errorf(`value "%s" is not integer`, inputValue)
		}
	case `float64`:
		if v, err := strconv.ParseFloat(inputValue.(string), 64); err == nil {
			return v, nil
		} else {
			return nil, fmt.Errorf(`value "%s" is not float`, inputValue)
		}
	}
	return inputValue, nil
}
