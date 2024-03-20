package definition

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func TestSwitchable(t *testing.T) {
	t.Parallel()

	var v SwitchableInterface = &Switchable{}
	assert.True(t, v.IsEnabled())

	now := utctime.MustParse("2006-01-02T15:04:05.000Z")

	// Disable
	v.Disable(now.Time(), "system", "some reason")
	assert.False(t, v.IsEnabled())
	assert.Empty(t, v.GetEnabledBy())
	assert.Nil(t, v.GetEnabledAt())
	assert.Equal(t, "system", v.GetDisabledBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.GetDisabledAt().String())
	assert.Equal(t, "some reason", v.GetDisabledReason())

	// Enable
	v.Enable(now.Time(), "user")
	assert.True(t, v.IsEnabled())
	assert.Equal(t, "user", v.GetEnabledBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.GetEnabledAt().String())
	assert.Empty(t, v.GetDisabledBy())
	assert.Nil(t, v.GetDisabledAt())
	assert.Empty(t, v.GetDisabledReason())

	// Disable
	v.Disable(now.Time(), "system", "some reason")
	assert.False(t, v.IsEnabled())
	assert.Empty(t, v.GetEnabledBy())
	assert.Nil(t, v.GetEnabledAt())
	assert.Equal(t, "system", v.GetDisabledBy())
	assert.Equal(t, "2006-01-02T15:04:05.000Z", v.GetDisabledAt().String())
	assert.Equal(t, "some reason", v.GetDisabledReason())
}
