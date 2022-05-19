package template

import (
	"context"
	"fmt"
	"sync"

	jsonnetLib "github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/ast"

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet/fsimporter"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/jsonnetfiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/metadata"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacevalues"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
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
	templateRef       model.TemplateRef
	instanceId        string
	instanceIdShort   string
	jsonNetCtx        *jsonnet.Context
	replacements      *replacevalues.Values
	inputs            map[string]InputValue
	tickets           *storageapi.TicketProvider
	placeholdersCount int
	ticketsResolved   bool

	lock         *sync.Mutex
	placeholders placeholdersMap
	objectIds    metadata.ObjectIdsMap
	inputsUsage  *metadata.InputsUsage
}

// placeholdersMap -  original template value -> placeholder.
type placeholdersMap map[interface{}]placeholder

type placeholder struct {
	asString string      // placeholder as string for use in Json file, eg. string("<<~~placeholder:1~~>>)
	asValue  interface{} // eg. ConfigId, RowId, eg. ConfigId("<<~~placeholder:1~~>>)
}

type placeholderResolver func(p placeholder, cb resolveCallback)

type resolveCallback func(newId interface{})

type inputUsageNotifier struct {
	*UseContext
	ctx context.Context
}

const (
	placeholderStart      = "<<~~"
	placeholderEnd        = "~~>>"
	instanceIdShortLength = 8
)

func NewUseContext(ctx context.Context, templateRef model.TemplateRef, objectsRoot filesystem.Fs, instanceId string, targetBranch model.BranchKey, inputs InputsValues, tickets *storageapi.TicketProvider) *UseContext {
	ctx = baseContext(ctx)
	c := &UseContext{
		_context:        ctx,
		templateRef:     templateRef,
		instanceId:      instanceId,
		instanceIdShort: strhelper.FirstN(instanceId, instanceIdShortLength),
		jsonNetCtx:      jsonnet.NewContext().WithCtx(ctx).WithImporter(fsimporter.New(objectsRoot)),
		replacements:    replacevalues.NewValues(),
		inputs:          make(map[string]InputValue),
		tickets:         tickets,
		lock:            &sync.Mutex{},
		placeholders:    make(placeholdersMap),
		objectIds:       make(metadata.ObjectIdsMap),
		inputsUsage:     metadata.NewInputsUsage(),
	}

	// Convert inputs to map
	for _, input := range inputs {
		c.inputs[input.Id] = input
	}

	// Replace BranchId, in template all objects have BranchId = 0
	c.replacements.AddKey(model.BranchKey{Id: 0}, targetBranch)

	// Register JsonNet functions: ConfigId, ConfigRowId, Input
	c.registerJsonNetFunctions()

	// Let's see where the inputs were used
	c.registerInputsUsageNotifier()

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

func (c *UseContext) ObjectIds() metadata.ObjectIdsMap {
	return c.objectIds
}

func (c *UseContext) InputsUsage() *metadata.InputsUsage {
	return c.inputsUsage
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
				switch v := v.Value.(type) {
				case int:
					return float64(v), nil
				default:
					return v, nil
				}
			}
		},
	})
	c.jsonNetCtx.NativeFunctionWithAlias(&jsonnet.NativeFunction{
		Name:   `InputIsAvailable`,
		Params: ast.Identifiers{"id"},
		Func: func(params []interface{}) (interface{}, error) {
			if len(params) != 1 {
				return nil, fmt.Errorf("one parameter expected, found %d", len(params))
			} else if id, ok := params[0].(string); !ok {
				return nil, fmt.Errorf("parameter must be a string")
			} else if v, found := c.inputs[id]; !found {
				return nil, fmt.Errorf(`input "%s" not found`, id)
			} else {
				return !v.Skipped, nil
			}
		},
	})

	// InstanceId
	c.jsonNetCtx.NativeFunctionWithAlias(&jsonnet.NativeFunction{
		Name:   `InstanceId`,
		Params: ast.Identifiers{},
		Func: func(params []interface{}) (interface{}, error) {
			return c.instanceId, nil
		},
	})
	c.jsonNetCtx.NativeFunctionWithAlias(&jsonnet.NativeFunction{
		Name:   `InstanceIdShort`,
		Params: ast.Identifiers{},
		Func: func(params []interface{}) (interface{}, error) {
			return c.instanceIdShort, nil
		},
	})
}

// ConfigId/ConfigRowId in JsonNet files is replaced by a <<~~ticket:123~~>> placeholder.
// When all JsonNet files are processed, new IDs are generated in parallel.
func (c *UseContext) idPlaceholder(oldId interface{}) string {
	p := c.registerPlaceholder(oldId, func(p placeholder, cb resolveCallback) {
		// Placeholder -> new ID
		var newId interface{}
		c.tickets.Request(func(ticket *model.Ticket) {
			switch p.asValue.(type) {
			case model.ConfigId:
				newId = model.ConfigId(ticket.Id)
			case model.RowId:
				newId = model.RowId(ticket.Id)
			default:
				panic(fmt.Errorf("unexpected ID type"))
			}
			cb(newId)
		})
	})
	return p.asString
}

// registerPlaceholder for an object oldId, it can be resolved later/async.
func (c *UseContext) registerPlaceholder(oldId interface{}, fn placeholderResolver) placeholder {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, found := c.placeholders[oldId]; !found {
		// Generate placeholder, it will be later replaced by a new ID
		c.placeholdersCount++
		p := placeholder{asString: fmt.Sprintf("%splaceholder:%d%s", placeholderStart, c.placeholdersCount, placeholderEnd)}

		// Convert string to an ID value
		switch oldId.(type) {
		case model.ConfigId:
			p.asValue = model.ConfigId(p.asString)
		case model.RowId:
			p.asValue = model.RowId(p.asString)
		default:
			panic(fmt.Errorf("unexpected ID type"))
		}

		// Store oldId -> placeholder
		c.placeholders[oldId] = p

		// Resolve newId async by provider function
		fn(p, func(newId interface{}) {
			c.replacements.AddId(p.asValue, newId)
			c.objectIds[newId] = oldId
		})
	}
	return c.placeholders[oldId]
}

func (c *UseContext) registerInputsUsageNotifier() {
	c.jsonNetCtx.NotifierFactory(func(ctx context.Context) jsonnetLib.Notifier {
		return &inputUsageNotifier{UseContext: c, ctx: ctx}
	})
}

func (c *inputUsageNotifier) OnGeneratedValue(fnName string, args []interface{}, _ interface{}, steps []interface{}) {
	// Only for Input function
	if fnName != "Input" {
		return
	}

	// One argument expected
	if len(args) != 1 {
		return
	}

	// Argument is input name
	inputName, ok := args[0].(string)
	if !ok {
		return
	}

	// Convert steps to orderedmap format
	var mappedSteps []orderedmap.Step
	for _, step := range steps {
		switch v := step.(type) {
		case jsonnetLib.ObjectFieldStep:
			mappedSteps = append(mappedSteps, orderedmap.MapStep(v.Field))
		case jsonnetLib.ArrayIndexStep:
			mappedSteps = append(mappedSteps, orderedmap.SliceStep(v.Index))
		default:
			panic(fmt.Errorf(`unexpected type "%T"`, v))
		}
	}

	// Get file definition
	fileDef, _ := c.ctx.Value(jsonnetfiles.FileDefCtxKey).(*filesystem.FileDef)
	if fileDef == nil {
		return
	}

	// Get key of the parent object
	objectKey, ok := fileDef.MetadataOrNil(filesystem.ObjectKeyMetadata).(model.Key)
	if !ok {
		return
	}

	// We are only interested in the inputs used in the configuration.
	if !fileDef.HasTag(model.FileKindObjectConfig) {
		return
	}

	// Replace tickets in object key
	objectKeyRaw, err := c.replacements.Replace(objectKey)
	if err != nil {
		panic(err)
	}

	// Store
	objectKey = objectKeyRaw.(model.Key)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.inputsUsage.Values[objectKey] = append(c.inputsUsage.Values[objectKey], metadata.InputUsage{
		Name:    inputName,
		JsonKey: mappedSteps,
	})
}
