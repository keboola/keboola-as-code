package definition_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestSoftDeletable(t *testing.T) {
	t.Parallel()

	var v definition.SoftDeletableInterface = &definition.SoftDeletable{}
	assert.False(t, v.IsDeleted())
	assert.False(t, v.IsDeletedDirectly())
	assert.Zero(t, v.DeletedAt())

	now := utctime.MustParse("2006-01-02T15:04:05.000Z").Time()
	by := test.ByUser()

	v.Delete(now, by, false)
	assert.True(t, v.IsDeleted())
	assert.False(t, v.IsDeletedDirectly())
	assert.Nil(t, v.UndeletedBy())
	assert.Zero(t, v.UndeletedAt())
	assert.Equal(t, &by, v.DeletedBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.DeletedAt().String())

	v.Undelete(now, by)
	assert.False(t, v.IsDeleted())
	assert.False(t, v.IsDeletedDirectly())
	assert.Nil(t, v.DeletedBy())
	assert.Zero(t, v.DeletedAt())
	assert.Equal(t, &by, v.UndeletedBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.UndeletedAt().String())

	v.Delete(now, by, true)
	assert.True(t, v.IsDeleted())
	assert.True(t, v.IsDeletedDirectly())
	assert.Nil(t, v.UndeletedBy())
	assert.Zero(t, v.UndeletedAt())
	assert.Equal(t, &by, v.DeletedBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.DeletedAt().String())
}

func TestSoftDeletable_Validation(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2006-01-02T15:04:05.000Z")
	by := test.ByUser()

	// Test cases
	cases := testvalidation.TestCases[definition.SoftDeletable]{
		{
			Name: "deleted/undeleted - nil",
			Value: definition.SoftDeletable{
				Deleted:   nil,
				Undeleted: nil,
			},
		},
		{
			Name: "deleted/undeleted - both",
			ExpectedError: `
- "deleted" is an excluded field
- "undeleted" is an excluded field
`,
			Value: definition.SoftDeletable{
				Deleted:   &definition.Deleted{},
				Undeleted: &definition.Undeleted{},
			},
		},
		{
			Name: "deleted - empty",
			ExpectedError: `
- "deleted.at" is a required field
- "deleted.by" is a required field
`,
			Value: definition.SoftDeletable{
				Deleted: &definition.Deleted{},
			},
		},
		{
			Name: "deleted, directly = true",
			Value: definition.SoftDeletable{
				Deleted: &definition.Deleted{Directly: true, At: now, By: by},
			},
		},
		{
			Name: "deleted, directly = false",
			Value: definition.SoftDeletable{
				Deleted: &definition.Deleted{Directly: false, At: now, By: by},
			},
		},
		{
			Name: "undeleted - empty",
			ExpectedError: `
- "undeleted.at" is a required field
- "undeleted.by" is a required field
`,
			Value: definition.SoftDeletable{
				Undeleted: &definition.Undeleted{},
			},
		},
		{
			Name: "undeleted, ok",
			Value: definition.SoftDeletable{
				Undeleted: &definition.Undeleted{At: now, By: by},
			},
		},
	}

	// Run test cases
	cases.Run(t)
}
