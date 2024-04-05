package definition_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestSwitchable(t *testing.T) {
	t.Parallel()

	var v definition.SwitchableInterface = &definition.Switchable{}
	assert.True(t, v.IsEnabled())

	now := utctime.MustParse("2006-01-02T15:04:05.000Z")
	by := test.ByUser()

	// Disable
	v.Disable(now.Time(), by, "some reason", false)
	assert.False(t, v.IsEnabled())
	assert.Nil(t, v.EntityEnabledBy())
	assert.Nil(t, v.EntityEnabledAt())
	assert.Equal(t, &by, v.EntityDisabledBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.EntityDisabledAt().String())
	assert.Equal(t, "some reason", v.EntityDisabledReason())

	// Enable
	v.Enable(now.Time(), by)
	assert.True(t, v.IsEnabled())
	assert.Equal(t, &by, v.EntityEnabledBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.EntityEnabledAt().String())
	assert.Nil(t, v.EntityDisabledBy())
	assert.Nil(t, v.EntityDisabledAt())
	assert.Empty(t, v.EntityDisabledReason())

	// Disable
	v.Disable(now.Time(), by, "some reason", true)
	assert.False(t, v.IsEnabled())
	assert.Nil(t, v.EntityEnabledBy())
	assert.Nil(t, v.EntityEnabledAt())
	assert.Equal(t, &by, v.EntityDisabledBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.EntityDisabledAt().String())
	assert.Equal(t, "some reason", v.EntityDisabledReason())
}

func TestSwitchable_Validation(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2006-01-02T15:04:05.000Z")
	by := test.ByUser()

	// Test cases
	cases := testvalidation.TestCases[definition.Switchable]{
		{
			Name: "disabled = false",
			Value: definition.Switchable{
				Disabled: false,
			},
		},
		{
			Name: "disabled = false, invalid",
			ExpectedError: `
- "disabledAt" should not be set
- "disabledReason" should not be set
- "disabledWithParent" should not be set
`,
			Value: definition.Switchable{
				Disabled:           false,
				DisabledWithParent: true,
				DisabledAt:         &now,
				DisabledReason:     "some reason",
			},
		},
		{
			Name: "disabled = true, disabledWithParent = true",
			Value: definition.Switchable{
				Disabled:           true,
				DisabledWithParent: true,
				DisabledBy:         &by,
				DisabledAt:         &now,
				DisabledReason:     "some reason",
			},
		},
		{
			Name: "disabled = true, disabledWithParent = true, invalid",
			ExpectedError: `
- "disabledBy" is a required field
- "disabledAt" is a required field
- "disabledReason" is a required field
`,
			Value: definition.Switchable{
				Disabled:           true,
				DisabledWithParent: true,
			},
		},
		{
			Name: "disabled = true, disabledWithParent = false, invalid",
			ExpectedError: `
- "disabledBy" is a required field
- "disabledAt" is a required field
- "disabledReason" is a required field
`,
			Value: definition.Switchable{
				Disabled:           true,
				DisabledWithParent: false,
			},
		},
	}

	// Run test cases
	cases.Run(t)
}
