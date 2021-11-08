package model_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
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

func TestRelationsEqual(t *testing.T) {
	t.Parallel()
	v1 := &fixtures.MockedApiSideRelation{OtherSide: fixtures.MockedKey{Id: `123`}}
	v2 := &fixtures.MockedApiSideRelation{OtherSide: fixtures.MockedKey{Id: `345`}}
	assert.True(t, (Relations{}).Equal(Relations{}))
	assert.True(t, (Relations{v1}).Equal(Relations{v1}))
	assert.True(t, (Relations{v1, v2}).Equal(Relations{v1, v2}))
	assert.True(t, (Relations{v2, v1}).Equal(Relations{v1, v2}))
	assert.False(t, (Relations{}).Equal(Relations{v1}))
	assert.False(t, (Relations{}).Equal(Relations{v1, v2}))
	assert.False(t, (Relations{v1}).Equal(Relations{v1, v2}))
	assert.False(t, (Relations{v2}).Equal(Relations{v1, v2}))
}

func TestRelationsDiff(t *testing.T) {
	t.Parallel()
	v1 := &fixtures.MockedApiSideRelation{OtherSide: fixtures.MockedKey{Id: `123`}}
	v2 := &fixtures.MockedApiSideRelation{OtherSide: fixtures.MockedKey{Id: `345`}}
	v3 := &fixtures.MockedApiSideRelation{OtherSide: fixtures.MockedKey{Id: `567`}}
	v4 := &fixtures.MockedApiSideRelation{OtherSide: fixtures.MockedKey{Id: `789`}}
	onlyIn1, onlyIn2 := (Relations{v1, v2, v3}).Diff(Relations{v2, v4})
	assert.Equal(t, Relations{v1, v3}, onlyIn1)
	assert.Equal(t, Relations{v4}, onlyIn2)
}

func TestRelationsOnlyStoredInManifest(t *testing.T) {
	t.Parallel()
	v1 := &fixtures.MockedApiSideRelation{OtherSide: fixtures.MockedKey{Id: `123`}}
	v2 := &fixtures.MockedManifestSideRelation{OtherSide: fixtures.MockedKey{Id: `345`}}
	v3 := &fixtures.MockedApiSideRelation{OtherSide: fixtures.MockedKey{Id: `567`}}
	v4 := &fixtures.MockedManifestSideRelation{OtherSide: fixtures.MockedKey{Id: `789`}}
	r := Relations{v1, v2, v3, v4}
	assert.Equal(t, Relations{v2, v4}, r.OnlyStoredInManifest())
}

func TestRelationsOnlyStoredInApi(t *testing.T) {
	t.Parallel()
	v1 := &fixtures.MockedApiSideRelation{OtherSide: fixtures.MockedKey{Id: `123`}}
	v2 := &fixtures.MockedManifestSideRelation{OtherSide: fixtures.MockedKey{Id: `345`}}
	v3 := &fixtures.MockedApiSideRelation{OtherSide: fixtures.MockedKey{Id: `567`}}
	v4 := &fixtures.MockedManifestSideRelation{OtherSide: fixtures.MockedKey{Id: `789`}}
	r := Relations{v1, v2, v3, v4}
	assert.Equal(t, Relations{v1, v3}, r.OnlyStoredInApi())
}

func TestVariablesForRelation(t *testing.T) {
	t.Parallel()

	r := &VariablesForRelation{
		ComponentId: `foo.bar`,
		Id:          `12345`,
	}

	// The relation is defined on this source side (variables config)
	definedOn := &Config{
		ConfigKey: ConfigKey{
			BranchId:    123,
			ComponentId: VariablesComponentId,
			Id:          `45678`,
		},
	}

	// Check other side key (regular component config, it uses variables)
	otherSideKey, otherSideRel, err := r.NewOtherSideRelation(definedOn, nil)
	assert.NoError(t, err)
	assert.Equal(t, ConfigKey{
		BranchId:    123, // from source key
		ComponentId: `foo.bar`,
		Id:          `12345`,
	}, otherSideKey)
	assert.Equal(t, &VariablesFromRelation{
		VariablesId: `45678`,
	}, otherSideRel)

	// ParentKey key, same as target, ... variables config is stored within component config
	parentKey, err := r.ParentKey(definedOn.Key())
	assert.NoError(t, err)
	assert.Equal(t, ConfigKey{
		BranchId:    123, // from source key
		ComponentId: `foo.bar`,
		Id:          `12345`,
	}, parentKey)
}
