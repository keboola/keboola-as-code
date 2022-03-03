package template

import (
	"context"
	"fmt"

	"github.com/google/go-jsonnet/ast"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacevalues"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// UseContext represents the process of replacing values when applying a template to a remote project.
//
// Process description:
//   1. There is some template.
//      - It contains objects IDs defined by functions, for example: ConfigId("my-config-id"), ConfigRowId("my-row-id")
//   2. When loading JsonNet files, functions are called.
//      - A placeholder is generated for each unique value.
//      - For example, each ConfigId("my-config-id") is replaced by "<<~~func:ticket:1~~>>".
//      - This is because we do not know in advance how many new IDs will need to be generated.
//      - Function call can contain an expression, for example ConfigId("my-config-" + tableName), and this prevents forward analysis.
//      - Functions are defined in UseContext.registerJsonNetFunctions().
//   3. When the entire template is loaded, the placeholders are replaced with new IDs.
//      - For example, each "<<~~func:ticket:1~~>>" is replaced by "3496482342".
//      - Replacements are defined by UseContext.Replacements().
//      - Values are replaced by "internal/pkg/mapper/template/replacevalues".
//    4. Then the objects are copied to the project,
//      - See "pkg/lib/operation/project/local/template/use/operation.go".
//      - A new path is generated for each new object, according to the project naming.
//
// UseContext.JsonNetContext() returns JsonNet functions.
// UseContext.Replacements() returns placeholders for new IDs.
type UseContext struct {
	_context
	templateRef     model.TemplateRef
	instanceId      string
	jsonNetCtx      *jsonnet.Context
	replacements    *replacevalues.Values
	inputs          map[string]interface{}
	tickets         *storageapi.TicketProvider
	ticketId        int
	ticketsResolved bool
	placeholders    map[interface{}]string
}

const (
	placeholderStart = "<<~~"
	placeholderEnd   = "~~>>"
)

func NewUseContext(ctx context.Context, templateRef model.TemplateRef, instanceId string, targetBranch model.BranchKey, inputs InputsValues, tickets *storageapi.TicketProvider) *UseContext {
	c := &UseContext{
		_context:     baseContext(ctx),
		templateRef:  templateRef,
		instanceId:   instanceId,
		jsonNetCtx:   jsonnet.NewContext(),
		replacements: replacevalues.NewValues(),
		inputs:       make(map[string]interface{}),
		tickets:      tickets,
		placeholders: make(map[interface{}]string),
	}

	// Convert inputs to map
	for _, input := range inputs {
		c.inputs[input.Id] = input.Value
	}

	// Replace BranchId, in template all objects have BranchId = 0
	c.replacements.AddKey(model.BranchKey{Id: 0}, targetBranch)

	// Register JsonNet functions: ConfigId, ConfigRowId, Input
	c.registerJsonNetFunctions()

	return c
}

func (c *UseContext) TemplateRef() model.TemplateRef {
	return c.templateRef
}

func (c *UseContext) InstanceId() string {
	return c.instanceId
}

func (c *UseContext) JsonNetContext() *jsonnet.Context {
	return c.jsonNetCtx
}

func (c *UseContext) Replacements() (*replacevalues.Values, error) {
	// Generate new IDs
	if !c.ticketsResolved {
		if err := c.tickets.Resolve(); err != nil {
			return nil, err
		}
		c.ticketsResolved = true
	}
	return c.replacements, nil
}

func (c *UseContext) RemoteObjectsFilter() model.ObjectsFilter {
	return model.NoFilter()
}

func (c *UseContext) LocalObjectsFilter() model.ObjectsFilter {
	return model.NoFilter()
}

func (c *UseContext) registerJsonNetFunctions() {
	// ConfigId
	c.jsonNetCtx.NativeFunctionWithAlias(&jsonnet.NativeFunction{
		Name:   `ConfigId`,
		Params: ast.Identifiers{"id"},
		Func: func(params []interface{}) (interface{}, error) {
			if len(params) != 1 {
				return nil, fmt.Errorf("one parameter expected, found %d", len(params))
			} else if id, ok := params[0].(string); !ok {
				return nil, fmt.Errorf("parameter must be a string")
			} else {
				return c.idPlaceholder(model.ConfigId(id)), nil
			}
		},
	})

	// ConfigRowId
	c.jsonNetCtx.NativeFunctionWithAlias(&jsonnet.NativeFunction{
		Name:   `ConfigRowId`,
		Params: ast.Identifiers{"id"},
		Func: func(params []interface{}) (interface{}, error) {
			if len(params) != 1 {
				return nil, fmt.Errorf("one parameter expected, found %d", len(params))
			} else if id, ok := params[0].(string); !ok {
				return nil, fmt.Errorf("parameter must be a string")
			} else {
				return c.idPlaceholder(model.RowId(id)), nil
			}
		},
	})

	// Inputs
	c.jsonNetCtx.NativeFunctionWithAlias(&jsonnet.NativeFunction{
		Name:   `Input`,
		Params: ast.Identifiers{"id"},
		Func: func(params []interface{}) (interface{}, error) {
			if len(params) != 1 {
				return nil, fmt.Errorf("one parameter expected, found %d", len(params))
			} else if id, ok := params[0].(string); !ok {
				return nil, fmt.Errorf("parameter must be a string")
			} else if v, found := c.inputs[id]; !found {
				return nil, fmt.Errorf(`input "%s" not found`, id)
			} else {
				switch v := v.(type) {
				case int:
					return float64(v), nil
				default:
					return v, nil
				}
			}
		},
	})
}

// ConfigId/ConfigRowId in JsonNet files is replaced by a <<~~ticket:123~~>> placeholder.
// When all JsonNet files are processed, new IDs are generated in parallel.
func (c *UseContext) idPlaceholder(old interface{}) string {
	if _, found := c.placeholders[old]; !found {
		// Generate placeholder, it will be later replaced by a new ID
		c.ticketId++
		placeholderStr := fmt.Sprintf("%sticket:%d%s", placeholderStart, c.ticketId, placeholderEnd)
		c.placeholders[old] = placeholderStr

		// Old -> placeholder
		var placeholder interface{}
		switch old.(type) {
		case model.ConfigId:
			placeholder = model.ConfigId(placeholderStr)
		case model.RowId:
			placeholder = model.RowId(placeholderStr)
		default:
			panic(fmt.Errorf("unexpected ID type"))
		}

		// Placeholder -> new ID
		c.tickets.Request(func(ticket *model.Ticket) {
			switch placeholder.(type) {
			case model.ConfigId:
				c.replacements.AddId(placeholder, model.ConfigId(ticket.Id))
			case model.RowId:
				c.replacements.AddId(placeholder, model.RowId(ticket.Id))
			default:
				panic(fmt.Errorf("unexpected ID type"))
			}
		})
	}

	return c.placeholders[old]
}
