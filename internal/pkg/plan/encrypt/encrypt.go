package encrypt

import (
	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// NewPlan creates a plan for encrypt all unencrypted values in all configs and rows.
func NewPlan(projectState *state.State) *Plan {
	builder := &encryptPlanBuilder{State: projectState}
	actions := builder.build()
	return &Plan{State: projectState, actions: actions}
}

type encryptPlanBuilder struct {
	*state.State
	actions []*action
}

func (b *encryptPlanBuilder) build() []*action {
	for _, object := range b.All() {
		b.processObject(object)
	}

	return b.actions
}

func (b *encryptPlanBuilder) processObject(objectState model.ObjectState) {
	// Only local objects
	if !objectState.HasLocalState() {
		return
	}

	// Only config or row
	if o, ok := objectState.LocalState().(model.ObjectWithContent); ok {
		// Wall through
		var values []*UnencryptedValue
		o.GetContent().VisitAllRecursive(func(path orderedmap.Key, value interface{}, parent interface{}) {
			if v, ok := value.(string); ok {
				if key, ok := path.Last().(orderedmap.MapStep); ok && encryption.IsKeyToEncrypt(key.String()) && !encryption.IsEncrypted(v) {
					values = append(values, &UnencryptedValue{path: path, value: v})
				}
			}
		})

		// Store action if some unencrypted values are found
		if len(values) > 0 {
			b.actions = append(b.actions, &action{
				ObjectState: objectState,
				object:      o,
				values:      values,
			})
		}
	}
}
