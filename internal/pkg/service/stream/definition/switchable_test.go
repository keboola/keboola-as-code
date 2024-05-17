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
	assert.Nil(t, v.EnabledBy())
	assert.Nil(t, v.EnabledAt())
	assert.Equal(t, &by, v.DisabledBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.DisabledAt().String())
	assert.Equal(t, "some reason", v.DisabledReason())

	// Enable
	v.Enable(now.Time(), by)
	assert.True(t, v.IsEnabled())
	assert.Equal(t, &by, v.EnabledBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.EnabledAt().String())
	assert.Nil(t, v.DisabledBy())
	assert.Nil(t, v.DisabledAt())
	assert.Empty(t, v.DisabledReason())

	// Disable
	v.Disable(now.Time(), by, "some reason", true)
	assert.False(t, v.IsEnabled())
	assert.Nil(t, v.EnabledBy())
	assert.Nil(t, v.EnabledAt())
	assert.Equal(t, &by, v.DisabledBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.DisabledAt().String())
	assert.Equal(t, "some reason", v.DisabledReason())
}

func TestSwitchable_Validation(t *testing.T) {
	t.Parallel()

	now := utctime.MustParse("2006-01-02T15:04:05.000Z")
	by := test.ByUser()

	// Test cases
	cases := testvalidation.TestCases[definition.Switchable]{
		{
			Name: "disabled/enabled - nil",
			Value: definition.Switchable{
				Disabled: nil,
				Enabled:  nil,
			},
		},
		{
			Name: "disabled/enabled - both",
			ExpectedError: `
- "disabled" is an excluded field
- "enabled" is an excluded field
`,
			Value: definition.Switchable{
				Disabled: &definition.Disabled{},
				Enabled:  &definition.Enabled{},
			},
		},
		{
			Name: "disabled - empty",
			ExpectedError: `
- "disabled.at" is a required field
- "disabled.reason" is a required field
- "disabled.by" is a required field
`,
			Value: definition.Switchable{
				Disabled: &definition.Disabled{},
			},
		},
		{
			Name: "disabled , directly = true",
			Value: definition.Switchable{
				Disabled: &definition.Disabled{
					Directly: true,
					At:       now,
					Reason:   "some reason",
					By:       by,
				},
			},
		},
		{
			Name: "disabled , directly = false",
			Value: definition.Switchable{
				Disabled: &definition.Disabled{
					Directly: false,
					At:       now,
					Reason:   "some reason",
					By:       by,
				},
			},
		},
		{
			Name: "enabled - empty",
			ExpectedError: `
- "enabled.at" is a required field
- "enabled.by" is a required field
`,
			Value: definition.Switchable{
				Enabled: &definition.Enabled{},
			},
		},
		{
			Name: "enabled , ok",
			Value: definition.Switchable{
				Enabled: &definition.Enabled{
					At: now,
					By: by,
				},
			},
		},
	}

	// Run test cases
	cases.Run(t)
}
