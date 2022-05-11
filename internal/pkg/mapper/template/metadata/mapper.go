package metadata

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type metadataMapper struct {
	state       *state.State
	templateRef model.TemplateRef
	instanceId  string
	objectIds   ObjectIdsMap
}

// ObjectIdsMap - generated object id -> template object id.
type ObjectIdsMap map[interface{}]interface{}

func (v ObjectIdsMap) IdInTemplate(idInProject interface{}) (interface{}, bool) {
	id, found := v[idInProject]
	return id, found
}

func NewMapper(state *state.State, templateRef model.TemplateRef, instanceId string, objectIds ObjectIdsMap) *metadataMapper {
	if instanceId == "" {
		panic(fmt.Errorf(`template "instanceId" cannot be empty`))
	}
	return &metadataMapper{state: state, templateRef: templateRef, instanceId: instanceId, objectIds: objectIds}
}
