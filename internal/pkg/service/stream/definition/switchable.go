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
	Disabled *Disabled `json:"disabled" validate:"excluded_with=Enabled"`
	Enabled  *Enabled  `json:"enabled" validate:"excluded_with=Disabled"`
}

type Disabled struct {
	By     By              `json:"by" validate:"required"`
	At     utctime.UTCTime `json:"at" validate:"required"`
	Reason string          `json:"reason" validate:"required"`
	// DisabledWithParent is true when the object has not been disabled directly but was disabled together with its parent.
	DisabledWithParent bool `json:"disabledWithParent"`
}

type Enabled struct {
	By By              `json:"by" validate:"required"`
	At utctime.UTCTime `json:"at" validate:"required"`
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
