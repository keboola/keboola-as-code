package definition

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test/testvalidation"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func TestVersioned(t *testing.T) {
	t.Parallel()

	var v VersionedInterface

	v = &Versioned{Version: Version{Number: 1, Hash: "f43e93acd97eceb3"}}
	assert.Equal(t, VersionNumber(1), v.VersionNumber())
	assert.Equal(t, "0000000001", v.VersionNumber().String())
	assert.Equal(t, "f43e93acd97eceb3", v.VersionHash())
}

func TestVersioned_IncrementVersion(t *testing.T) {
	t.Parallel()
	now := time.Now()

	// Increment version number from 0
	bar := "bar"
	entity0 := &TestVersionedEntity{Foo: "bar", Bar: &bar}
	entity0.IncrementVersion(entity0, now, "initialization")
	hash0 := entity0.VersionHash()
	assert.Equal(t, utctime.From(now), entity0.VersionModifiedAt())
	assert.Equal(t, "initialization", entity0.VersionDescription())
	assert.Equal(t, VersionNumber(1), entity0.VersionNumber())
	assert.Equal(t, "f43e93acd97eceb3", hash0)

	// Increment version number, generate new hash
	entity1 := &TestVersionedEntity{
		Foo:       "bar",
		Bar:       &bar,
		Versioned: Versioned{Version: Version{Number: 123, Hash: "abc"}},
	}
	entity1.IncrementVersion(entity1, now, "new version")
	hash1 := entity1.VersionHash()
	assert.Equal(t, utctime.From(now), entity1.VersionModifiedAt())
	assert.Equal(t, "new version", entity1.VersionDescription())
	assert.Equal(t, VersionNumber(124), entity1.VersionNumber())
	assert.Equal(t, "f43e93acd97eceb3", hash1)
	assert.NotEmpty(t, hash1)
	assert.NotEqual(t, "abc", hash1)
	assert.Equal(t, hash0, hash1)

	// Hash doesn't depends on the previous version number or hash
	entity2 := &TestVersionedEntity{
		Foo:       "bar",
		Bar:       &bar,
		Versioned: Versioned{Version: Version{Number: 456, Hash: "def"}},
	}
	entity2.IncrementVersion(entity2, now, "")
	hash2 := entity2.VersionHash()
	assert.Equal(t, VersionNumber(457), entity2.VersionNumber())
	assert.Equal(t, "f43e93acd97eceb3", hash2)
	assert.Equal(t, hash1, hash2)

	// hash depends on the entity fields values
	entity3 := &TestVersionedEntity{
		Foo:       "different value",
		Bar:       &bar,
		Versioned: Versioned{Version: Version{Number: 456, Hash: "def"}},
	}
	entity3.IncrementVersion(entity3, now, "")
	hash3 := entity3.VersionHash()
	assert.Equal(t, VersionNumber(457), entity3.VersionNumber())
	assert.Equal(t, "3b609ea2ebfa1afc", hash3)
	assert.NotEqual(t, hash2, hash3)
}

func TestVersioned_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := testvalidation.TestCases[Versioned]{
		{
			Name: "empty",
			ExpectedError: `
- "version.number" is a required field
- "version.hash" is a required field
- "version.modifiedAt" is a required field
`,
			Value: Versioned{},
		},
		{
			Name: "ok",
			Value: Versioned{
				Version: Version{
					Number:     1,
					Hash:       "0123456789123456",
					ModifiedAt: utctime.From(time.Now()),
				},
			},
		},
	}

	// Run test cases
	cases.Run(t)
}

type TestVersionedEntity struct {
	Versioned
	Foo string
	Bar *string
}
