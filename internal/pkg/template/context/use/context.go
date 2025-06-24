package

// Package use represents the process of replacing of values when applying a template.
use

import (
	"context"
	"fmt"

	jsonnetLib "github.com/google/go-jsonnet"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/sasha-s/go-deadlock"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet/fsimporter"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/jsonnetfiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/metadata"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacevalues"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/jsonnet/function"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ulid"
)

// Context represents the process of the replacing values when applying a template.
//
// Process description:
//  1. There is some template.
//     - It contains objects IDs defined by functions, for example: ConfigId("my-config-id"), ConfigRowId("my-row-id")
//  2. When loading Jsonnet files, functions are called.
//     - A placeholder is generated for each unique value.
//     - For example, each ConfigId("my-config-id") is replaced by "<<~~func:ticket:1~~>>".
//     - This is because we do not know in advance how many new IDs will need to be generated.
//     - Function call can contain an expression, for example ConfigId("my-config-" + tableName), and this prevents forward analysis.
//     - Functions are defined in Context.registerJsonnetFunctions().
//  3. When the entire template is loaded, the placeholders are replaced with new IDs.
//     - For example, each "<<~~func:ticket:1~~>>" is replaced by "3496482342".
//     - Replacements are defined by Context.Replacements().
//     - Values are replaced by "internal/pkg/mapper/template/replacevalues".
//  4. Then the objects are copied to the project,
//     - See "pkg/lib/operation/project/local/template/use/operation.go".
//     - A new path is generated for each new object, according to the project naming.
//
// Context.JsonnetContext() returns Jsonnet functions.
// Context.Replacements() returns placeholders for new IDs.
type Context struct {
	_context
	templateRef       model.TemplateRef
	instanceID        string
	instanceIDShort   string
	jsonnetCtx        *jsonnet.Context
	replacements      *replacevalues.Values
	inputsValues      map[string]template.InputValue
	components        *model.ComponentsMap
	placeholdersCount int
	projectBackends   []string
	idGenerator       ulid.Generator

	lock          *deadlock.Mutex
	placeholders  PlaceholdersMap
	objectIds     metadata.ObjectIdsMap
	inputsUsage   *metadata.InputsUsage
	inputsDefsMap map[string]*template.Input
}

type _context context.Context

// PlaceholdersMap -  original template value -> placeholder.
type PlaceholdersMap map[any]Placeholder

type Placeholder struct {
	asString string // placeholder as string for use in Json file, eg. string("<<~~placeholder:1~~>>)
	asValue  any    // eg. ConfigId, RowID, eg. ConfigId("<<~~placeholder:1~~>>)
}

func (v Placeholder) Value() any {
	return v.asValue
}

type PlaceholderResolver func(p Placeholder, cb ResolveCallback)

type ResolveCallback func(newID any)

type inputUsageNotifier struct {
	*Context
	ctx context.Context
}

const (
	placeholderStart      = "<<~~"
	placeholderEnd        = "~~>>"
	instanceIDShortLength = 8
)

func NewContext(
	ctx context.Context,
	templateRef model.TemplateRef,
	objectsRoot filesystem.Fs,
	instanceID string,
	targetBranch model.BranchKey,
	inputsValues template.InputsValues,
	inputsDefsMap map[string]*template.Input,
	components *model.ComponentsMap,
	projectState *state.State,
	projectBackends []string,
	idGenerator ulid.Generator,
) *Context {
	ctx = template.NewContext(ctx)
	c := &Context{
		_context:        ctx,
		templateRef:     templateRef,
		instanceID:      instanceID,
		instanceIDShort: strhelper.FirstN(instanceID, instanceIDShortLength),
		jsonnetCtx:      jsonnet.NewContext().WithCtx(ctx).WithImporter(fsimporter.New(objectsRoot)),
		replacements:    replacevalues.NewValues(),
		inputsValues:    make(map[string]template.InputValue),
		components:      components,
		lock:            &deadlock.Mutex{},
		placeholders:    make(PlaceholdersMap),
		objectIds:       make(metadata.ObjectIdsMap),
		inputsUsage:     metadata.NewInputsUsage(),
		inputsDefsMap:   inputsDefsMap,
		projectBackends: projectBackends,
		idGenerator:     idGenerator,
	}

	// Convert inputsValues to map
	for _, input := range inputsValues {
		c.inputsValues[input.ID] = input
	}

	// Replace BranchID, in template all objects have BranchID = 0
	c.replacements.AddKey(model.BranchKey{ID: 0}, targetBranch)

	// Register Jsonnet functions
	c.registerJsonnetFunctions()

	// Let's see where the inputs were used
	c.registerInputsUsageNotifier()

	// Register IDs of shaded codes, each shared code is one row.
	for _, config := range projectState.LocalObjects().ConfigsWithRowsFrom(targetBranch) {
		rowsIdsMap := make(map[keboola.RowID]model.RowIDMetadata)
		for _, v := range config.Metadata.RowsTemplateIds() {
			rowsIdsMap[v.IDInProject] = v
		}
		if config.ComponentID == keboola.SharedCodeComponentID {
			for _, row := range config.Rows {
				if meta, found := rowsIdsMap[row.ID]; found {
					c.RegisterPlaceholder(meta.IDInTemplate, func(_ Placeholder, cb ResolveCallback) { cb(row.ID) })
				}
			}
		}
	}

	return c
}

func (c *Context) TemplateRef() model.TemplateRef {
	return c.templateRef
}

func (c *Context) InstanceID() string {
	return c.instanceID
}

func (c *Context) JsonnetContext() *jsonnet.Context {
	return c.jsonnetCtx
}

// ReplaceContentField sets nested value in config/row.Content ordered map.
func (c *Context) ReplaceContentField(objectKey model.Key, fieldPath orderedmap.Path, replace any) {
	c.replacements.AddContentField(objectKey, fieldPath, replace)
}

func (c *Context) Placeholders() PlaceholdersMap {
	return c.placeholders
}

func (c *Context) Replacements() (*replacevalues.Values, error) {
	// New IDs are generated on-the-fly in mapID
	return c.replacements, nil
}

func (c *Context) RemoteObjectsFilter() model.ObjectsFilter {
	return model.NoFilter()
}

func (c *Context) LocalObjectsFilter() model.ObjectsFilter {
	return model.NoFilter()
}

func (c *Context) ObjectIds() metadata.ObjectIdsMap {
	return c.objectIds
}

func (c *Context) InputsUsage() *metadata.InputsUsage {
	return c.inputsUsage
}

// RegisterPlaceholder for an object oldId, it can be resolved later/async.
func (c *Context) RegisterPlaceholder(oldID any, fn PlaceholderResolver) Placeholder {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, found := c.placeholders[oldID]; !found {
		// Generate placeholder, it will be later replaced by a new ID
		c.placeholdersCount++
		p := Placeholder{asString: fmt.Sprintf("%splaceholder:%d%s", placeholderStart, c.placeholdersCount, placeholderEnd)}

		// Convert string to an ID value
		switch oldID.(type) {
		case keboola.ConfigID:
			p.asValue = keboola.ConfigID(p.asString)
		case keboola.RowID:
			p.asValue = keboola.RowID(p.asString)
		default:
			panic(errors.New("unexpected ID type"))
		}

		// Store oldID -> placeholder
		c.placeholders[oldID] = p

		// Resolve newId async by provider function
		fn(p, func(newId any) {
			c.replacements.AddID(p.asValue, newId)
			c.objectIds[newId] = oldID
		})
	}
	return c.placeholders[oldID]
}

func (c *Context) registerJsonnetFunctions() {
	c.jsonnetCtx.NativeFunctionWithAlias(function.ConfigID(c.mapID))
	c.jsonnetCtx.NativeFunctionWithAlias(function.ConfigRowID(c.mapID))
	c.jsonnetCtx.NativeFunctionWithAlias(function.Input(c.inputValue))
	c.jsonnetCtx.NativeFunctionWithAlias(function.InputIsAvailable(c.inputValue))
	c.jsonnetCtx.NativeFunctionWithAlias(function.InstanceID(c.instanceID))
	c.jsonnetCtx.NativeFunctionWithAlias(function.InstanceIDShort(c.instanceIDShort))
	c.jsonnetCtx.NativeFunctionWithAlias(function.ComponentIsAvailable(c.components))
	c.jsonnetCtx.NativeFunctionWithAlias(function.SnowflakeWriterComponentID(c.components, c.projectBackends))
	c.jsonnetCtx.NativeFunctionWithAlias(function.HasProjectBackend(c.projectBackends))
	c.jsonnetCtx.NativeFunctionWithAlias(function.RandomID())
}

// mapID maps ConfigId/ConfigRowId in Jsonnet files to a <<~~ticket:123~~>> placeholder.
// When all Jsonnet files are processed, new IDs are generated in parallel.
func (c *Context) mapID(oldID any) string {
	p := c.RegisterPlaceholder(oldID, func(p Placeholder, cb ResolveCallback) {
		// Placeholder -> new ID
		var newID any
		// Generate ULID using the generator
		generatedID := c.idGenerator.NewULID()

		switch p.asValue.(type) {
		case keboola.ConfigID:
			newID = keboola.ConfigID(generatedID)
		case keboola.RowID:
			newID = keboola.RowID(generatedID)
		default:
			panic(errors.New("unexpected ID type"))
		}
		cb(newID)
	})
	return p.asString
}

func (c *Context) inputValue(inputID string) (template.InputValue, bool) {
	v, ok := c.inputsValues[inputID]
	return v, ok
}

func (c *Context) registerInputsUsageNotifier() {
	c.jsonnetCtx.NotifierFactory(func(ctx context.Context) jsonnetLib.Notifier {
		return &inputUsageNotifier{Context: c, ctx: ctx}
	})
}

func (n *inputUsageNotifier) OnGeneratedValue(fnName string, args []any, partial bool, partialValue, _ any, steps []any) {
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

	// Check if input exists and has been filled in by user
	if input, found := n.inputsValues[inputName]; !found || input.Skipped {
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
			panic(errors.Errorf(`unexpected type "%T"`, v))
		}
	}

	// Get file definition
	fileDef, _ := n.ctx.Value(jsonnetfiles.FileDefCtxKey).(*filesystem.FileDef)
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
	objectKeyRaw, err := n.replacements.Replace(objectKey)
	if err != nil {
		panic(err)
	}

	// Store
	objectKey = objectKeyRaw.(model.Key)
	n.lock.Lock()
	defer n.lock.Unlock()
	if !partial {
		// Values has been generated by the Input function, store input usage
		n.inputsUsage.Values[objectKey] = append(n.inputsUsage.Values[objectKey], metadata.InputUsage{
			Name:    inputName,
			JSONKey: mappedSteps,
			Def:     n.inputsDefsMap[inputName],
		})
	} else if jsonObject, ok := partialValue.(map[string]any); ok && len(jsonObject) > 0 {
		// Get JSON keys
		var keys []string
		for jsonKey := range jsonObject {
			keys = append(keys, jsonKey)
		}

		// Part of the object has been generated by the Input function, store input usage
		n.inputsUsage.Values[objectKey] = append(n.inputsUsage.Values[objectKey], metadata.InputUsage{
			Name:       inputName,
			JSONKey:    mappedSteps,
			Def:        n.inputsDefsMap[inputName],
			ObjectKeys: keys,
		})
	}
}
