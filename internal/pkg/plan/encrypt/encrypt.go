package encrypt

import (
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// NewPlan creates a plan for encrypt all unencrypted values in all configs and rows.
func NewPlan(objects model.ObjectStates) *Plan {
	builder := &encryptPlanBuilder{objects: objects}
	actions := builder.build()
	return &Plan{actions: actions}
}

type encryptPlanBuilder struct {
	objects model.ObjectStates
	actions []*action
}

func (b *encryptPlanBuilder) build() []*action {
	for _, object := range b.objects.All() {
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
		o.GetContent().VisitAllRecursive(func(path orderedmap.Path, value any, parent any) {
			if v, ok := value.(string); ok {
				if key, ok := path.Last().(orderedmap.MapStep); ok && keboola.IsKeyToEncrypt(key.Key()) && !keboola.IsEncrypted(v) {
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
