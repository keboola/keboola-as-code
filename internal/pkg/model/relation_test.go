package model

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRelationsUnmarshalJSON(t *testing.T) {
	t.Parallel()
	data := []byte(fmt.Sprintf(`[{"type": "%s"}]`, VariablesIdRelType))
	var relations Relations
	assert.NoError(t, json.Unmarshal(data, &relations))
	assert.Len(t, relations, 1)
	assert.IsType(t, &VariablesIdRelation{}, relations[0])
	assert.Equal(t, VariablesIdRelType, relations[0].Type())
}
