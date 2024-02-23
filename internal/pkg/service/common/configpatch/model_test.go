package configpatch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPatchKVs_With(t *testing.T) {
	t.Parallel()

	v1 := PatchKVs{
		{KeyPath: "key1", Value: "value1"},
		{KeyPath: "key2", Value: "value2"},
		{KeyPath: "key3", Value: "value3"},
	}
	v2 := PatchKVs{
		{KeyPath: "key3", Value: "FINAL"},
		{KeyPath: "key1", Value: "REWRITTEN"},
	}
	v3 := PatchKVs{
		{KeyPath: "key1", Value: "FINAL"},
	}

	// Merge and deduplicate, the later value has priority
	assert.Equal(t, PatchKVs{
		{KeyPath: "key1", Value: "FINAL"},
		{KeyPath: "key2", Value: "value2"},
		{KeyPath: "key3", Value: "FINAL"},
	}, v1.With(v2, v3))
}
