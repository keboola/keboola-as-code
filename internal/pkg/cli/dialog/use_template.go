package dialog

import (
	"context"
	"fmt"
	"reflect"
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
		switch i.Kind {
		case input.KindInput, input.KindPassword, input.KindTextarea:
			question := &prompt.Question{
				Label:       i.Name,
				Description: i.Description,
				Validator: func(raw interface{}) error {
					value := raw
					switch i.Type {
					case `int`:
						if v, err := strconv.Atoi(value.(string)); err == nil {
							value = v
						} else {
							return fmt.Errorf(`value "%s" is not integer`, raw)
						}
					case `float64`:
						if v, err := strconv.ParseFloat(value.(string), 64); err != nil {
							value = v
						} else {
							return fmt.Errorf(`value "%s" is not float`, raw)
						}
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
			results[i.Id] = value
		case input.KindConfirm:
			confirm := &prompt.Confirm{
				Label:       i.Name,
				Description: i.Description,
				Default:     i.Default.(bool),
			}
			if !reflect.ValueOf(&i.Default).IsZero() {
				confirm.Default = i.Default.(bool)
			}
			value := p.Confirm(confirm)
			ctx = context.WithValue(ctx, contextKey(i.Id), value)
			results[i.Id] = value
		case input.KindSelect:
			value, _ := p.Select(&prompt.Select{
				Label:       i.Name,
				Description: i.Description,
				Options:     i.Options,
				Default:     i.Default.(string),
				UseDefault:  true,
				Validator: func(val interface{}) error {
					return i.ValidateUserInput(val, ctx)
				},
			})
			ctx = context.WithValue(ctx, contextKey(i.Id), value)
			results[i.Id] = value
		case input.KindMultiSelect:
			value, _ := p.MultiSelect(&prompt.MultiSelect{
				Label:       i.Name,
				Description: i.Description,
				Options:     i.Options,
				Default:     i.Default.([]string),
				Validator: func(val interface{}) error {
					return i.ValidateUserInput(val, ctx)
				},
			})
			ctx = context.WithValue(ctx, contextKey(i.Id), value)
			results[i.Id] = value
		}
	}

	return results, errors.ErrorOrNil()
}
