package diff

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type typeName string

type Differ struct {
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
)

type Result struct {
	model.ObjectState
	State         ResultState
	ChangedFields model.ChangedFields
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
			state.Key(),
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

func (d *Differ) diffValues(objectKey model.Key, remoteValue, localValue interface{}) *Reporter {
	reporter := newReporter(objectKey, d.state.State)
	cmp.Diff(remoteValue, localValue, d.newOptions(reporter))
	return reporter
}

func (d *Differ) newOptions(reporter *Reporter) cmp.Options {
	return cmp.Options{
		cmp.Reporter(reporter),
		// Compare Config/ConfigRow configuration content ("orderedmap" type) as map (keys order doesn't matter)
		cmp.Transformer("orderedmap", utils.OrderedMapToMap),
		// Compare strings by line by line
		cmpopts.AcyclicTransformer("strByLine", func(s string) []string {
			return strings.Split(s, "\n")
		}),
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
