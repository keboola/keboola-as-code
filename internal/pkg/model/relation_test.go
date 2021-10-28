package model

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRelationsUnmarshalJSON(t *testing.T) {
	t.Parallel()
	data := []byte(fmt.Sprintf(`[{"type": "%s"}]`, VariablesForRelType))
	var relations Relations
	assert.NoError(t, json.Unmarshal(data, &relations))
	assert.Len(t, relations, 1)
	assert.IsType(t, &VariablesForRelation{}, relations[0])
}

func TestRelationsMarshalJSON(t *testing.T) {
	t.Parallel()
	relations := Relations{&VariablesForRelation{}}
	data, err := json.Marshal(&relations)
	assert.NoError(t, err)
	assert.Contains(t, string(data), fmt.Sprintf(`"type":"%s"`, VariablesForRelType))
}

func TestVariablesForRelation(t *testing.T) {
	t.Parallel()
	raw, err := newEmptyRelation(VariablesForRelType)
	assert.NoError(t, err)
	r, ok := raw.(*VariablesForRelation)
	assert.True(t, ok)
	r.Target = ConfigKeySameBranch{
		ComponentId: `foo.bar`,
		Id:          `12345`,
	}

	// The relation is defined on this source side (variables config)
	sourceKey := ConfigKey{
		BranchId:    123,
		ComponentId: `bar.baz`,
		Id:          `45678`,
	}

	// And we want the target side key (regular component config, it uses variables)
	targetKey, err := r.TargetKey(sourceKey)
	assert.NoError(t, err)
	assert.Equal(t, ConfigKey{
		BranchId:    123, // from source key
		ComponentId: `foo.bar`,
		Id:          `12345`,
	}, targetKey)

	// Parent key, same as target, ... variables config is stored within component config
	parentKey, err := r.ParentKey(sourceKey)
	assert.NoError(t, err)
	assert.Equal(t, ConfigKey{
		BranchId:    123, // from source key
		ComponentId: `foo.bar`,
		Id:          `12345`,
	}, parentKey)
}
