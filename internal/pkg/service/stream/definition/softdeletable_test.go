package definition_test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestSoftDeletable(t *testing.T) {
	t.Parallel()

	var v definition.SoftDeletableInterface = &definition.SoftDeletable{}
	assert.False(t, v.IsDeleted())
	assert.False(t, v.WasDeletedWithParent())
	assert.Nil(t, v.EntityDeletedAt())

	now := utctime.MustParse("2006-01-02T15:04:05.000Z").Time()
	by := test.ByUser()

	v.Delete(now, by, false)
	assert.True(t, v.IsDeleted())
	assert.False(t, v.WasDeletedWithParent())
	assert.Nil(t, v.EntityUndeletedBy())
	assert.Nil(t, v.EntityUndeletedAt())
	assert.Equal(t, &by, v.EntityDeletedBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.EntityDeletedAt().String())

	v.Undelete(now, by)
	assert.False(t, v.IsDeleted())
	assert.False(t, v.WasDeletedWithParent())
	assert.Nil(t, v.EntityDeletedBy())
	assert.Nil(t, v.EntityDeletedAt())
	assert.Equal(t, &by, v.EntityUndeletedBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.EntityUndeletedAt().String())

	v.Delete(now, by, true)
	assert.True(t, v.IsDeleted())
	assert.True(t, v.WasDeletedWithParent())
	assert.Nil(t, v.EntityUndeletedBy())
	assert.Nil(t, v.EntityUndeletedAt())
	assert.Equal(t, &by, v.EntityDeletedBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.EntityDeletedAt().String())
}

func TestSoftDeletable_Validation(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2006-01-02T15:04:05.000Z")
	by := test.ByUser()

	// Test cases
	cases := testvalidation.TestCases[definition.SoftDeletable]{
		{
			Name: "deleted = false",
			Value: definition.SoftDeletable{
				Deleted: false,
			},
		},
		{
			Name: "deleted = false, invalid",
			ExpectedError: `
- "deletedAt" should not be set
- "deletedWithParent" should not be set
`,
			Value: definition.SoftDeletable{
				Deleted:           false,
				DeletedWithParent: true,
				DeletedAt:         &now,
			},
		},
		{
			Name: "deleted = true, deletedWithParent = true",
			Value: definition.SoftDeletable{
				Deleted:           true,
				DeletedWithParent: true,
				DeletedBy:         &by,
				DeletedAt:         &now,
			},
		},
		{
			Name: "deleted = true, deletedWithParent = true, invalid",
			ExpectedError: `
- "deletedBy" is a required field
- "deletedAt" is a required field
`,
			Value: definition.SoftDeletable{
				Deleted:           true,
				DeletedWithParent: true,
			},
		},
		{
			Name: "deleted = true, deletedWithParent = false, invalid",
			ExpectedError: `
- "deletedBy" is a required field
- "deletedAt" is a required field
`,
			Value: definition.SoftDeletable{
				Deleted:           true,
				DeletedWithParent: false,
			},
		},
	}

	// Run test cases
	cases.Run(t)
}
