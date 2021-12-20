package dialog

import (
	"context"
	"fmt"
	"reflect"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (p *Dialogs) AskUseTemplateOptions(inputs input.Inputs) (results map[string]interface{}, err error) {
	results = make(map[string]interface{})
	ctx := context.Background()
	errors := utils.NewMultiError()
	for _, i := range inputs {
		switch i.Kind {
		case "input", "password", "textarea":
			question := &prompt.Question{
				Label:       i.Name,
				Description: i.Description,
				Validator: func(val interface{}) error {
					fmt.Printf("LOG %#v %#v\n", val, i)
					return i.ValidateUserInput(val, ctx)
				},
				Hidden: i.Kind == "password",
			}
			if i.Default != nil {
				question.Default = i.Default.(string)
			}
			value, ok := p.Ask(question)
			if !ok {
				errors.Append(fmt.Errorf(""))
			}
			ctx = context.WithValue(ctx, i.Name, value)
			results[i.Name] = value
		case "confirm":
			confirm := &prompt.Confirm{
				Label:       i.Name,
				Description: i.Description,
				Default:     i.Default.(bool),
			}
			if !reflect.ValueOf(&i.Default).IsZero() {
				confirm.Default = i.Default.(bool)
			}
			value := p.Confirm(confirm)
			ctx = context.WithValue(ctx, i.Name, value)
			results[i.Name] = value
		case "select":
			value, ok := p.Select(&prompt.Select{
				Label:       i.Name,
				Description: i.Description,
				Options:     i.Options,
				Default:     i.Default.(string),
				UseDefault:  true,
				Validator: func(val interface{}) error {
					return i.ValidateUserInput(val, ctx)
				},
			})
			if !ok {

			}
			ctx = context.WithValue(ctx, i.Name, value)
			results[i.Name] = value
		case "multiselect":
			value, ok := p.MultiSelect(&prompt.MultiSelect{
				Label:       i.Name,
				Description: i.Description,
				Options:     i.Options,
				Default:     i.Default.([]string),
				Validator: func(val interface{}) error {
					return i.ValidateUserInput(val, ctx)
				},
			})
			if !ok {

			}
			ctx = context.WithValue(ctx, i.Name, value)
			results[i.Name] = value
		}
	}

	return results, errors
}
