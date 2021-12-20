package diff

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type typeName string

type Differ struct {
	objects   model.ObjectStates
	results   []*Result                         // diff results
	typeCache map[typeName][]*utils.StructField // reflection cache
	errors    *utils.MultiError                 // errors
}

type ResultState int

const (
	EqualMark                    = "="
	ChangeMark                   = "*"
	AddMark                      = "+"
	DeleteMark                   = "×"
	OnlyInRemoteMark             = "-"
	OnlyInLocalMark              = "+"
	ResultNotSet     ResultState = iota
	ResultNotEqual
	ResultEqual
	ResultOnlyInRemote
	ResultOnlyInLocal
)

type Result struct {
	model.ObjectState
	State         ResultState
	ChangedFields model.ChangedFields
}

type Results struct {
	Equal                 bool
	HasNotEqualResult     bool
	HasOnlyInRemoteResult bool
	HasOnlyInLocalResult  bool
	Results               []*Result
	Objects               model.ObjectStates
}

func NewDiffer(objects model.ObjectStates) *Differ {
	return &Differ{
		objects:   objects,
		typeCache: make(map[typeName][]*utils.StructField),
	}
}

func (d *Differ) Diff() (*Results, error) {
	d.results = []*Result{}
	d.errors = utils.NewMultiError()

	// Diff all objects : branches, config, configRows
	results := &Results{Equal: true, Results: d.results, Objects: d.objects}
	for _, objectState := range d.objects.All() {
		result, err := d.diffState(objectState)
		if err != nil {
			d.errors.Append(err)
		} else {
			if result.State != ResultEqual {
				results.Equal = false
			}
			if result.State == ResultNotEqual {
				results.HasNotEqualResult = true
			}
			if result.State != ResultOnlyInRemote {
				results.HasOnlyInRemoteResult = true
			}
			if result.State != ResultOnlyInLocal {
				results.HasOnlyInLocalResult = true
			}
			d.results = append(d.results, result)
		}
	}

	// Sort results
	sort.SliceStable(d.results, func(i, j int) bool {
		return d.results[i].Path() < d.results[j].Path()
	})

	results.Results = d.results
	return results, d.errors.ErrorOrNil()
}

func (d *Differ) diffState(state model.ObjectState) (*Result, error) {
	result := &Result{ObjectState: state}
	result.ChangedFields = model.NewChangedFields()

	// Are both, Remote and Local state defined?
	if !state.HasRemoteState() && !state.HasLocalState() {
		panic(fmt.Errorf("both local and remote state are not set"))
	}

	// Not in remote state
	if !state.HasRemoteState() {
		result.State = ResultOnlyInLocal
		return result, nil
	}

	// Not in local state
	if !state.HasLocalState() {
		result.State = ResultOnlyInRemote
		return result, nil
	}

	remoteState := state.RemoteState()
	localState := state.LocalState()
	remoteType := reflect.TypeOf(remoteState).Elem()
	localType := reflect.TypeOf(localState).Elem()
	remoteValues := reflect.ValueOf(remoteState)
	localValues := reflect.ValueOf(localState)

	// Remote and Local types must be same
	if remoteType.String() != localType.String() {
		panic(fmt.Errorf("local(%s) and remote(%s) states must have same data type", remoteType, localType))
	}

	// Get available fields for diff, defined in `diff:"true"` tag in struct
	diffFields := d.getDiffFields(remoteType)
	if len(diffFields) == 0 {
		return nil, fmt.Errorf(`no field with tag "diff:true" in struct "%s"`, remoteType.String())
	}

	// Get pointer value
	if remoteValues.Type().Kind() == reflect.Ptr {
		remoteValues = remoteValues.Elem()
	}
	if localValues.Type().Kind() == reflect.Ptr {
		localValues = localValues.Elem()
	}

	// Diff
	for _, field := range diffFields {
		reporter := d.diffValues(
			state,
			remoteValues.FieldByName(field.StructField.Name).Interface(),
			localValues.FieldByName(field.StructField.Name).Interface(),
		)
		diffStr := reporter.String()
		if len(diffStr) > 0 {
			result.ChangedFields.
				Add(strhelper.FirstLower(field.JsonName())).
				SetDiff(diffStr).
				AddPath(reporter.Paths()...)
		}
	}

	if len(result.ChangedFields) > 0 {
		result.State = ResultNotEqual
	} else {
		result.State = ResultEqual
	}

	return result, nil
}

func (d *Differ) diffValues(objectState model.ObjectState, remoteValue, localValue interface{}) *Reporter {
	reporter := newReporter(objectState, d.objects)
	cmp.Diff(remoteValue, localValue, d.newOptions(reporter))
	return reporter
}

func (d *Differ) newOptions(reporter *Reporter) cmp.Options {
	return cmp.Options{
		cmp.Reporter(reporter),
		// Compare Config/ConfigRow configuration content ("orderedmap" type) as map (keys order doesn't matter)
		cmp.Transformer("orderedmap", func(m *orderedmap.OrderedMap) map[string]interface{} {
			return m.ToMap()
		}),
		// Separately compares the relations for the manifest and API side
		cmpopts.AcyclicTransformer("relations", func(relations model.Relations) model.RelationsBySide {
			return relations.RelationsBySide()
		}),
		// Diff transformation blocks as string
		cmp.Transformer("block", func(block model.Block) string {
			return block.String()
		}),
		// Diff orchestrator phases as string
		cmp.Transformer("phase", func(phase model.Phase) string {
			return phase.String()
		}),
		// Diff SharedCode row as string
		cmp.Transformer("sharedCodeRow", func(code model.SharedCodeRow) string {
			return code.String()
		}),
		// Do not compare local paths
		cmpopts.IgnoreTypes(model.PathInProject{}),
	}
}

func (d *Differ) getDiffFields(t reflect.Type) []*utils.StructField {
	if v, ok := d.typeCache[typeName(t.Name())]; ok {
		return v
	} else {
		diffFields := utils.GetFieldsWithTag("diff:true", t)
		name := typeName(t.Name())
		d.typeCache[name] = diffFields
		return diffFields
	}
}

func (r *Results) Format(details bool) []string {
	var out []string
	for _, result := range r.Results {
		if result.State != ResultEqual {
			// Message
			msg := fmt.Sprintf("%s %s %s", result.Mark(), result.Kind().Abbr, result.Path())
			if !details && !result.ChangedFields.IsEmpty() {
				msg += " | changed: " + result.ChangedFields.String()
			}
			out = append(out, msg)

			// Changed fields
			if details {
				for name, field := range result.ChangedFields {
					out = append(out, fmt.Sprintf("  %s:", name))
					for _, line := range strings.Split(field.Diff(), "\n") {
						out = append(out, fmt.Sprintf("  %s", line))
					}
				}
			}
		}
	}
	return out
}

func (r *Result) Mark() string {
	switch r.State {
	case ResultNotSet:
		return "?"
	case ResultNotEqual:
		return "*"
	case ResultEqual:
		return "="
	case ResultOnlyInRemote:
		return OnlyInRemoteMark
	case ResultOnlyInLocal:
		return OnlyInLocalMark
	default:
		panic(fmt.Errorf("unexpected type %T", r.State))
	}
}
