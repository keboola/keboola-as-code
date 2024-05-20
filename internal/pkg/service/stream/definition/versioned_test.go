package definition_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestVersioned(t *testing.T) {
	t.Parallel()

	var v definition.VersionedInterface = &definition.Versioned{Version: definition.Version{Number: 1, Hash: "f95525c9785b3fcb"}}
	assert.Equal(t, definition.VersionNumber(1), v.VersionNumber())
	assert.Equal(t, "0000000001", v.VersionNumber().String())
	assert.Equal(t, "f95525c9785b3fcb", v.VersionHash())
}

func TestVersioned_IncrementVersion(t *testing.T) {
	t.Parallel()

	now := time.Now()
	by := test.ByUser()

	// Increment version number from 0
	bar := "bar"
	entity0 := &TestVersionedEntity{Foo: "bar", Bar: &bar}
	entity0.IncrementVersion(entity0, now, by, "initialization")
	hash0 := entity0.VersionHash()
	assert.Equal(t, utctime.From(now), entity0.VersionModifiedAt())
	assert.Equal(t, "initialization", entity0.VersionDescription())
	assert.Equal(t, definition.VersionNumber(1), entity0.VersionNumber())
	assert.Equal(t, "f43e93acd97eceb3", hash0)

	// Increment version number, generate new hash
	entity1 := &TestVersionedEntity{
		Foo:       "bar",
		Bar:       &bar,
		Versioned: definition.Versioned{Version: definition.Version{Number: 123, Hash: "abc"}},
	}
	entity1.IncrementVersion(entity1, now, by, "new version")
	hash1 := entity1.VersionHash()
	assert.Equal(t, utctime.From(now), entity1.VersionModifiedAt())
	assert.Equal(t, "new version", entity1.VersionDescription())
	assert.Equal(t, definition.VersionNumber(124), entity1.VersionNumber())
	assert.Equal(t, "f43e93acd97eceb3", hash1)
	assert.NotEmpty(t, hash1)
	assert.NotEqual(t, "abc", hash1)
	assert.Equal(t, hash0, hash1)

	// Hash doesn't depends on the previous version number or hash
	entity2 := &TestVersionedEntity{
		Foo:       "bar",
		Bar:       &bar,
		Versioned: definition.Versioned{Version: definition.Version{Number: 456, Hash: "def"}},
	}
	entity2.IncrementVersion(entity2, now, by, "")
	hash2 := entity2.VersionHash()
	assert.Equal(t, definition.VersionNumber(457), entity2.VersionNumber())
	assert.Equal(t, "f43e93acd97eceb3", hash2)
	assert.Equal(t, hash1, hash2)

	// hash depends on the entity fields values
	entity3 := &TestVersionedEntity{
		Foo:       "different value",
		Bar:       &bar,
		Versioned: definition.Versioned{Version: definition.Version{Number: 456, Hash: "def"}},
	}
	entity3.IncrementVersion(entity3, now, by, "")
	hash3 := entity3.VersionHash()
	assert.Equal(t, definition.VersionNumber(457), entity3.VersionNumber())
	assert.Equal(t, "3b609ea2ebfa1afc", hash3)
	assert.NotEqual(t, hash2, hash3)
}

func TestVersioned_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := testvalidation.TestCases[definition.Versioned]{
		{
			Name: "empty",
			ExpectedError: `
- "version.number" is a required field
- "version.hash" is a required field
- "version.at" is a required field
- "version.by" is a required field
`,
			Value: definition.Versioned{},
		},
		{
			Name:  "ok",
			Value: test.Versioned(),
		},
	}

	// Run test cases
	cases.Run(t)
}

type TestVersionedEntity struct {
	definition.Versioned
	Foo string
	Bar *string
}
