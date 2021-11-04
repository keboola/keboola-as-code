package diff

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type typeName string

type Differ struct {
	reporter  Reporter
	state     *state.State                      // model state
	results   []*Result                         // diff results
	typeCache map[typeName][]*utils.StructField // reflection cache
	error     *utils.Error                      // errors
}

type ResultState int

const (
	OnlyInRemoteMark             = "-"
	OnlyInLocalMark              = "+"
	ResultNotSet     ResultState = iota
	ResultNotEqual
	ResultEqual
	ResultOnlyInRemote
	ResultOnlyInLocal
	ResultInvisible // present in remote state, but ignored
)

type Result struct {
	model.ObjectState
	State         ResultState
	ChangedFields []string
	Differences   map[string]string
}

type Results struct {
	CurrentState *state.State
	Equal        bool
	Results      []*Result
}

func NewDiffer(state *state.State) *Differ {
	return &Differ{
		state:     state,
		typeCache: make(map[typeName][]*utils.StructField),
	}
}

func (d *Differ) Diff() (*Results, error) {
	d.results = []*Result{}
	d.error = utils.NewMultiError()

	// Diff all objects in state: branches, config, configRows
	equal := true
	for _, objectState := range d.state.All() {
		result, err := d.diffState(objectState)
		if err != nil {
			d.error.Append(err)
		} else {
			if result.State != ResultEqual {
				equal = false
			}
			d.results = append(d.results, result)
		}
	}

	// Check errors
	var err error
	if d.error.Len() > 0 {
		err = fmt.Errorf("%s", d.error)
	}

	return &Results{CurrentState: d.state, Equal: equal, Results: d.results}, err
}

func (d *Differ) diffState(state model.ObjectState) (*Result, error) {
	result := &Result{ObjectState: state}
	result.ChangedFields = make([]string, 0)
	result.Differences = make(map[string]string)

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
		difference := d.diffValues(
			remoteState,
			remoteValues.FieldByName(field.StructField.Name).Interface(),
			localState,
			localValues.FieldByName(field.StructField.Name).Interface(),
		)
		if len(difference) > 0 {
			result.ChangedFields = append(result.ChangedFields, field.JsonName())
			result.Differences[field.JsonName()] = difference
		}
	}

	if len(result.ChangedFields) > 0 {
		result.State = ResultNotEqual
	} else {
		result.State = ResultEqual
	}

	return result, nil
}

func (d *Differ) diffValues(remoteState, remoteValue, localState, localValue interface{}) string {
	d.reporter = newReporter(remoteState, localState)
	cmp.Diff(remoteValue, localValue, d.newOptions())
	return d.reporter.String()
}

func (d *Differ) newOptions() cmp.Options {
	return cmp.Options{
		// Compare Config/ConfigRow configuration content ("orderedmap" type) as map (keys order doesn't matter)
		cmp.Transformer("orderedmap", utils.OrderedMapToMap),
		// Compare relations in human-readable form
		cmp.Transformer("relations", func(relations model.Relations) string {
			var out []string
			for _, relation := range relations {
				out = append(out, d.relationToString(relation))
			}
			sort.Strings(out)
			return strings.Join(out, "\n")
		}),
		// Compare strings by lines
		cmpopts.AcyclicTransformer("strByLine", func(s string) []string {
			return strings.Split(s, "\n")
		}),
		// Custom reporter
		cmp.Reporter(&d.reporter),
	}
}

func (d *Differ) relationToString(relation model.Relation) string {
	otherSideDesc := ``
	if relationParent := d.reporter.ParentKey(); relationParent != nil {
		otherSideKey := relation.OtherSideKey(relationParent)
		if otherSide, found := d.state.Get(otherSideKey); found {
			otherSideDesc = `"` + otherSide.Path() + `"`
		}
		if len(otherSideDesc) == 0 {
			otherSideDesc = otherSideKey.Desc()
		}
	}
	return relation.Desc() + ` ` + otherSideDesc
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

func (r *Result) Mark() string {
	switch r.State {
	case ResultNotSet:
		return "? "
	case ResultNotEqual:
		return "CH"
	case ResultEqual:
		return "= "
	case ResultOnlyInRemote:
		return OnlyInRemoteMark + " "
	case ResultOnlyInLocal:
		return OnlyInLocalMark + " "
	default:
		panic(fmt.Errorf("unexpected type %T", r.State))
	}
}
