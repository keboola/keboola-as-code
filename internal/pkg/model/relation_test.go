package model_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRelationsUnmarshalJSON(t *testing.T) {
	t.Parallel()
	data := []byte(fmt.Sprintf(`[{"type": "%s"}]`, VariablesForRelType))
	var relations Relations
	require.NoError(t, json.Unmarshal(data, &relations))
	assert.Len(t, relations, 1)
	assert.IsType(t, &VariablesForRelation{}, relations[0])
}

func TestRelationsMarshalJSON(t *testing.T) {
	t.Parallel()
	relations := Relations{&VariablesForRelation{}}
	data, err := json.Marshal(&relations)
	require.NoError(t, err)
	assert.Contains(t, string(data), fmt.Sprintf(`"type":"%s"`, VariablesForRelType))
}

func TestRelationsEqual(t *testing.T) {
	t.Parallel()
	v1 := &fixtures.MockedAPISideRelation{OtherSide: fixtures.MockedKey{ID: `123`}}
	v2 := &fixtures.MockedAPISideRelation{OtherSide: fixtures.MockedKey{ID: `345`}}
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
	v1 := &fixtures.MockedAPISideRelation{OtherSide: fixtures.MockedKey{ID: `123`}}
	v2 := &fixtures.MockedAPISideRelation{OtherSide: fixtures.MockedKey{ID: `345`}}
	v3 := &fixtures.MockedAPISideRelation{OtherSide: fixtures.MockedKey{ID: `567`}}
	v4 := &fixtures.MockedAPISideRelation{OtherSide: fixtures.MockedKey{ID: `789`}}
	onlyIn1, onlyIn2 := (Relations{v1, v2, v3}).Diff(Relations{v2, v4})
	assert.Equal(t, Relations{v1, v3}, onlyIn1)
	assert.Equal(t, Relations{v4}, onlyIn2)
}

func TestRelationsOnlyStoredInManifest(t *testing.T) {
	t.Parallel()
	v1 := &fixtures.MockedAPISideRelation{OtherSide: fixtures.MockedKey{ID: `123`}}
	v2 := &fixtures.MockedManifestSideRelation{OtherSide: fixtures.MockedKey{ID: `345`}}
	v3 := &fixtures.MockedAPISideRelation{OtherSide: fixtures.MockedKey{ID: `567`}}
	v4 := &fixtures.MockedManifestSideRelation{OtherSide: fixtures.MockedKey{ID: `789`}}
	r := Relations{v1, v2, v3, v4}
	assert.Equal(t, Relations{v2, v4}, r.OnlyStoredInManifest())
}

func TestRelationsOnlyStoredInAPI(t *testing.T) {
	t.Parallel()
	v1 := &fixtures.MockedAPISideRelation{OtherSide: fixtures.MockedKey{ID: `123`}}
	v2 := &fixtures.MockedManifestSideRelation{OtherSide: fixtures.MockedKey{ID: `345`}}
	v3 := &fixtures.MockedAPISideRelation{OtherSide: fixtures.MockedKey{ID: `567`}}
	v4 := &fixtures.MockedManifestSideRelation{OtherSide: fixtures.MockedKey{ID: `789`}}
	r := Relations{v1, v2, v3, v4}
	assert.Equal(t, Relations{v1, v3}, r.OnlyStoredInAPI())
}

func TestVariablesForRelation(t *testing.T) {
	t.Parallel()

	r := &VariablesForRelation{
		ComponentID: `foo.bar`,
		ConfigID:    `12345`,
	}

	// The relation is defined on this source side (variables config)
	definedOn := &Config{
		ConfigKey: ConfigKey{
			BranchID:    123,
			ComponentID: keboola.VariablesComponentID,
			ID:          `45678`,
		},
	}

	// Check other side key (regular component config, it uses variables)
	otherSideKey, otherSideRel, err := r.NewOtherSideRelation(definedOn, nil)
	require.NoError(t, err)
	assert.Equal(t, ConfigKey{
		BranchID:    123, // from source key
		ComponentID: `foo.bar`,
		ID:          `12345`,
	}, otherSideKey)
	assert.Equal(t, &VariablesFromRelation{
		VariablesID: `45678`,
	}, otherSideRel)

	// ParentKey key, same as target, ... variables config is stored within component config
	parentKey, err := r.ParentKey(definedOn.Key())
	require.NoError(t, err)
	assert.Equal(t, ConfigKey{
		BranchID:    123, // from source key
		ComponentID: `foo.bar`,
		ID:          `12345`,
	}, parentKey)
}
