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
}

func NewMapper(state *state.State, templateRef model.TemplateRef, instanceId string) *metadataMapper {
	if instanceId == "" {
		panic(fmt.Errorf(`template "instanceId" cannot be empty`))
	}
	return &metadataMapper{state: state, templateRef: templateRef, instanceId: instanceId}
}
