package plan

import (
	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// Encrypt creates a plan for encrypt all unencrypted values in all configs and rows.
func Encrypt(projectState *state.State) *EncryptPlan {
	builder := &encryptPlanBuilder{State: projectState}
	actions := builder.build()
	return &EncryptPlan{naming: projectState.Naming(), actions: actions}
}

type encryptPlanBuilder struct {
	*state.State
	actions []*EncryptAction
}

type encryptActionBuilder struct {
	values []*UnencryptedValue
}

func (b *encryptPlanBuilder) build() []*EncryptAction {
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
		// Wall through AND store if some unencrypted values are found
		builder := &encryptActionBuilder{}
		builder.processValue(o.GetContent(), nil)
		if len(builder.values) > 0 {
			b.actions = append(b.actions, &EncryptAction{
				ObjectState: objectState,
				object:      o,
				values:      builder.values,
			})
		}
	}
}

func (b *encryptActionBuilder) processValue(value interface{}, path utils.KeyPath) {
	switch v := value.(type) {
	case *orderedmap.OrderedMap:
		b.processMap(v, path)
	case orderedmap.OrderedMap:
		b.processMap(&v, path)
	case []interface{}:
		b.processSlice(v, path)
	case string:
		lastStep := path.LastStep()
		if s, ok := lastStep.(utils.MapStep); ok && encryption.IsKeyToEncrypt(s.String()) && !encryption.IsEncrypted(v) {
			b.values = append(b.values, &UnencryptedValue{path: path, value: v})
		}
	}
}

func (b *encryptActionBuilder) processMap(content *orderedmap.OrderedMap, path utils.KeyPath) {
	for _, key := range content.Keys() {
		value, _ := content.Get(key)
		b.processValue(value, append(path, utils.MapStep(key)))
	}
}

func (b *encryptActionBuilder) processSlice(slice []interface{}, path utils.KeyPath) {
	for i, value := range slice {
		b.processValue(value, append(path, utils.SliceStep(i)))
	}
}
