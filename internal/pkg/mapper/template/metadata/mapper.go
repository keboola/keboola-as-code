package metadata

import (
	"fmt"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type metadataMapper struct {
	state       *state.State
	templateRef model.TemplateRef
	instanceId  string
	objectIds   ObjectIdsMap
	inputsUsage *InputsUsage
}

// ObjectIdsMap - generated object id -> template object id.
type ObjectIdsMap map[interface{}]interface{}

// InputsUsage contains all uses of inputs per object.
type InputsUsage struct {
	Values InputsUsageMap
}

type InputsUsageMap map[model.Key][]InputUsage

// InputUsage describes where the input is used in the output JSON.
type InputUsage struct {
	Name    string
	JsonKey orderedmap.Path
}

func (v ObjectIdsMap) IdInTemplate(idInProject interface{}) (interface{}, bool) {
	id, found := v[idInProject]
	return id, found
}

func NewInputsUsage() *InputsUsage {
	return &InputsUsage{
		Values: make(map[model.Key][]InputUsage),
	}
}

func NewMapper(state *state.State, templateRef model.TemplateRef, instanceId string, objectIds ObjectIdsMap, inputsUsage *InputsUsage) *metadataMapper {
	if instanceId == "" {
		panic(fmt.Errorf(`template "instanceId" cannot be empty`))
	}
	return &metadataMapper{state: state, templateRef: templateRef, instanceId: instanceId, objectIds: objectIds, inputsUsage: inputsUsage}
}
