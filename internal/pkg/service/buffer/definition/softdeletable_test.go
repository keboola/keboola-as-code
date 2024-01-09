package definition

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test/testvalidation"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func TestSoftDeletable(t *testing.T) {
	t.Parallel()

	var v SoftDeletableInterface = &SoftDeletable{}
	assert.False(t, v.IsDeleted())
	assert.False(t, v.WasDeletedWithParent())
	assert.Nil(t, v.GetDeletedAt())

	now := utctime.MustParse("2006-01-02T15:04:05.000Z").Time()

	v.Delete(now, false)
	assert.True(t, v.IsDeleted())
	assert.False(t, v.WasDeletedWithParent())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.GetDeletedAt().String())

	v.Undelete()
	assert.False(t, v.IsDeleted())
	assert.False(t, v.WasDeletedWithParent())
	assert.Nil(t, v.GetDeletedAt())

	v.Delete(now, true)
	assert.True(t, v.IsDeleted())
	assert.True(t, v.WasDeletedWithParent())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.GetDeletedAt().String())
}

func TestSoftDeletable_Validation(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2006-01-02T15:04:05.000Z")

	// Test cases
	cases := testvalidation.TestCases[SoftDeletable]{
		{
			Name: "deleted = false",
			Value: SoftDeletable{
				Deleted: false,
			},
		},
		{
			Name: "deleted = false, invalid",
			ExpectedError: `
- "deletedAt" should not be set
- "deletedWithParent" should not be set
`,
			Value: SoftDeletable{
				Deleted:           false,
				DeletedWithParent: true,
				DeletedAt:         &now,
			},
		},
		{
			Name: "deleted = true, deletedWithParent = true",
			Value: SoftDeletable{
				Deleted:           true,
				DeletedWithParent: true,
				DeletedAt:         &now,
			},
		},
		{
			Name:          "deleted = true, deletedWithParent = true, invalid",
			ExpectedError: `"deletedAt" is a required field`,
			Value: SoftDeletable{
				Deleted:           true,
				DeletedWithParent: true,
			},
		},
		{
			Name:          "deleted = true, deletedWithParent = false, invalid",
			ExpectedError: `"deletedAt" is a required field`,
			Value: SoftDeletable{
				Deleted:           true,
				DeletedWithParent: false,
			},
		},
	}

	// Run test cases
	cases.Run(t)
}
