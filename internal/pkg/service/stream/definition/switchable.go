package definition

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type SwitchableInterface interface {
	Enable(now time.Time, by By)
	Disable(now time.Time, by By, reason string, disabledWithParent bool)
	IsEnabled() bool
	EntityDisabledBy() *By
	EntityDisabledAt() *utctime.UTCTime
	EntityDisabledReason() string
	EntityEnabledBy() *By
	EntityEnabledAt() *utctime.UTCTime
}

type Switchable struct {
	Disabled       bool             `json:"disabled,omitempty"`
	DisabledBy     *By              `json:"disabledBy,omitempty" validate:"required_if=Disabled true,excluded_if=Disabled false"`
	DisabledAt     *utctime.UTCTime `json:"disabledAt,omitempty"  validate:"required_if=Disabled true,excluded_if=Disabled false"`
	DisabledReason string           `json:"disabledReason,omitempty"  validate:"required_if=Disabled true,excluded_if=Disabled false"`
	// DisabledWithParent is true when the object has not been disabled directly but was disabled together with its parent.
	DisabledWithParent bool             `json:"disabledWithParent,omitempty" validate:"excluded_if=Disabled false"`
	EnabledBy          *By              `json:"enabledBy,omitempty" validate:"excluded_if=Disabled true"`
	EnabledAt          *utctime.UTCTime `json:"enabledAt,omitempty"  validate:"excluded_if=Disabled true"`
}

func (v *Switchable) IsEnabled() bool {
	return !v.Disabled
}

func (v *Switchable) Enable(now time.Time, by By) {
	at := utctime.From(now)
	v.Disabled = false
	v.DisabledBy = nil
	v.DisabledAt = nil
	v.DisabledReason = ""
	v.DisabledWithParent = false
	v.EnabledBy = &by
	v.EnabledAt = &at
}

func (v *Switchable) Disable(now time.Time, by By, reason string, disabledWithParent bool) {
	at := utctime.From(now)
	v.Disabled = true
	v.DisabledBy = &by
	v.DisabledAt = &at
	v.DisabledReason = reason
	v.DisabledWithParent = disabledWithParent
	v.EnabledBy = nil
	v.EnabledAt = nil
}

func (v *Switchable) EntityDisabledBy() *By {
	return v.DisabledBy
}

func (v *Switchable) EntityDisabledAt() *utctime.UTCTime {
	return v.DisabledAt
}

func (v *Switchable) EntityDisabledReason() string {
	return v.DisabledReason
}

func (v *Switchable) EntityEnabledBy() *By {
	return v.EnabledBy
}

func (v *Switchable) EntityEnabledAt() *utctime.UTCTime {
	return v.EnabledAt
}
