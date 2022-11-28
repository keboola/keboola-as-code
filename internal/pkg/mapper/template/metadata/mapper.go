package metadata

import (
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type metadataMapper struct {
	state       *state.State
	templateRef model.TemplateRef
	instanceID  string
	objectIds   ObjectIdsMap
	inputsUsage *InputsUsage
}

// ObjectIdsMap - generated object id -> template object id.
type ObjectIdsMap map[interface{}]interface{}

// InputsUsage contains all uses of inputs per object.
type InputsUsage struct {
	Values InputsUsageMap
}

// OAuthConfigsMap returns input names mapped to oauth configurations.
func (u InputsUsage) OAuthConfigsMap() map[string]model.ConfigKey {
	res := map[string]model.ConfigKey{}
	for key, usages := range u.Values {
		for _, u := range usages {
			if u.Def.Kind == input.KindOAuth {
				res[u.Name] = key.(model.ConfigKey)
			}
		}
	}
	return res
}

type InputsUsageMap map[model.Key][]InputUsage

// InputUsage describes where the input is used in the output JSON.
type InputUsage struct {
	Name       string
	JSONKey    orderedmap.Path
	Def        *input.Input
	ObjectKeys []string // list of object keys generated from the input (empty = all)
}

func (v ObjectIdsMap) IDInTemplate(idInProject interface{}) (interface{}, bool) {
	id, found := v[idInProject]
	return id, found
}

func NewInputsUsage() *InputsUsage {
	return &InputsUsage{
		Values: make(map[model.Key][]InputUsage),
	}
}

func NewMapper(state *state.State, templateRef model.TemplateRef, instanceID string, objectIds ObjectIdsMap, inputsUsage *InputsUsage) *metadataMapper {
	if instanceID == "" {
		panic(errors.New(`template "instanceId" cannot be empty`))
	}
	return &metadataMapper{state: state, templateRef: templateRef, instanceID: instanceID, objectIds: objectIds, inputsUsage: inputsUsage}
}
