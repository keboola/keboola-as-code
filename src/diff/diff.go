package diff

import (
	"fmt"
	"github.com/google/go-cmp/cmp"
	"github.com/iancoleman/orderedmap"
	"keboola-as-code/src/json"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
	"reflect"
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
	ResultNotSet ResultState = iota
	ResultNotEqual
	ResultEqual
	ResultOnlyInRemote
	ResultOnlyInLocal
)

type Result struct {
	state.ObjectState
	State         ResultState
	ChangedFields []string
	Differences   map[string]string
}

type Results struct {
	Results []*Result
}

func NewDiffer(state *state.State) *Differ {
	return &Differ{
		state:     state,
		typeCache: make(map[typeName][]*utils.StructField),
	}
}

func (d *Differ) Diff() (*Results, error) {
	d.results = []*Result{}
	d.error = &utils.Error{}

	// Diff all objects in state: branches, config, configRows
	for _, objectState := range d.state.All() {
		result, err := d.doDiff(objectState)
		if err != nil {
			d.error.Add(err)
		} else {
			d.results = append(d.results, result)
		}
	}

	// Check errors
	var err error
	if d.error.Len() > 0 {
		err = fmt.Errorf("%s", d.error)
	}

	return &Results{d.results}, err
}

func (d *Differ) doDiff(state state.ObjectState) (*Result, error) {
	result := &Result{ObjectState: state}
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

	// Are both, Remote and Local state defined?
	result.ChangedFields = make([]string, 0)
	result.Differences = make(map[string]string)
	if remoteValues.IsNil() && localValues.IsNil() {
		panic(fmt.Errorf("both local and remote state are not set"))
	}
	if remoteValues.IsNil() {
		result.State = ResultOnlyInLocal
		return result, nil
	}
	if localValues.IsNil() {
		result.State = ResultOnlyInRemote
		return result, nil
	}

	// Get pointer value
	if remoteValues.Type().Kind() == reflect.Ptr {
		remoteValues = remoteValues.Elem()
	}
	if localValues.Type().Kind() == reflect.Ptr {
		localValues = localValues.Elem()
	}

	// Compare Config/ConfigRow configuration content ("orderedmap" type) as string
	configTransform := cmp.Transformer("orderedmap", func(m *orderedmap.OrderedMap) string {
		str, err := json.EncodeString(m, true)
		if err != nil {
			panic(fmt.Errorf("cannot encode JSON: %s", err))
		}
		return str
	})

	// Diff
	for _, field := range diffFields {
		difference := cmp.Diff(
			remoteValues.FieldByName(field.StructField.Name).Interface(),
			localValues.FieldByName(field.StructField.Name).Interface(),
			configTransform,
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
