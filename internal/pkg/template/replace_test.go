package template

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestKeysReplacement_Values(t *testing.T) {
	t.Parallel()
	keys := KeysReplacement{
		{
			Old: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `12`,
			},
			New: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `23`,
			},
		},
		{
			Old: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `12`,
				Id:          `45`,
			},
			New: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `23`,
				Id:          `67`,
			},
		},
	}
	values, err := keys.Values()
	assert.NoError(t, err)
	assert.Equal(t, ValuesReplacement{
		ValueReplacement{
			Old: model.ConfigKey{BranchId: 1, ComponentId: "foo.bar", Id: "12"},
			New: model.ConfigKey{BranchId: 1, ComponentId: "foo.bar", Id: "23"},
		},
		ValueReplacement{
			Old: model.ConfigId("12"),
			New: model.ConfigId("23"),
		},
		ValueReplacement{
			Old: model.ConfigRowKey{BranchId: 1, ComponentId: "foo.bar", ConfigId: "12", Id: "45"},
			New: model.ConfigRowKey{BranchId: 1, ComponentId: "foo.bar", ConfigId: "23", Id: "67"},
		},
		ValueReplacement{
			Old: model.RowId("45"),
			New: model.RowId("67"),
		},
	}, values)
}

func TestKeysReplacement_Values_DuplicateOld(t *testing.T) {
	t.Parallel()
	keys := KeysReplacement{
		{
			Old: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `12`, // <<<<<<<<<<<<<
			},
			New: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `23`,
			},
		},
		{
			Old: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `12`,
				Id:          `12`, // <<<<<<<<<<<<<
			},
			New: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `23`,
				Id:          `67`,
			},
		},
	}
	_, err := keys.Values()
	assert.Error(t, err)
	assert.Equal(t, `the old ID "12" is defined 2x`, err.Error())
}

func TestKeysReplacement_Values_DuplicateNew(t *testing.T) {
	t.Parallel()
	keys := KeysReplacement{
		{
			Old: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `12`,
			},
			New: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `23`, // <<<<<<<<<<<<<
			},
		},
		{
			Old: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `12`,
				Id:          `45`,
			},
			New: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `23`,
				Id:          `23`, // <<<<<<<<<<<<<
			},
		},
	}
	_, err := keys.Values()
	assert.Error(t, err)
	assert.Equal(t, `the new ID "23" is defined 2x`, err.Error())
}
