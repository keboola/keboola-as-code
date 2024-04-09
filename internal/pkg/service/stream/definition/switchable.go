package definition

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type SwitchableInterface interface {
	// Enable marks the entity as enabled.
	Enable(now time.Time, by By)
	// Disable marks the entity as disabled.
	Disable(now time.Time, by By, reason string, disabledWithParent bool)
	// IsEnabled returns true if the entity is marked as enabled.
	IsEnabled() bool
	IsEnabledAt(at time.Time) bool
	// IsDisabled returns true if the entity is marked as disabled.
	IsDisabled() bool
	IsDisabledAt(at time.Time) bool
	// IsDisabledDirectly returns true if the entity has been disabled directly, not together with its parent.
	IsDisabledDirectly() bool
	DisabledBy() *By
	DisabledAt() *utctime.UTCTime
	DisabledReason() string
	EnabledBy() *By
	EnabledAt() *utctime.UTCTime
}

type Switchable struct {
	Disabled *Disabled `json:"disabled,omitempty" validate:"excluded_with=Enabled"`
	Enabled  *Enabled  `json:"enabled,omitempty" validate:"excluded_with=Disabled"`
}

type Disabled struct {
	// Directly is true if the entity has been disabled directly, not together with its parent.
	Directly bool            `json:"directly"`
	At       utctime.UTCTime `json:"at" validate:"required"`
	Reason   string          `json:"reason" validate:"required"`
	By       By              `json:"by" validate:"required"`
}

type Enabled struct {
	At utctime.UTCTime `json:"at" validate:"required"`
	By By              `json:"by" validate:"required"`
}

func (v *Switchable) Enable(now time.Time, by By) {
	v.Disabled = nil
	v.Enabled = &Enabled{At: utctime.From(now), By: by}
}

func (v *Switchable) Disable(now time.Time, by By, reason string, directly bool) {
	v.Disabled = &Disabled{At: utctime.From(now), By: by, Reason: reason, Directly: directly}
	v.Enabled = nil
}

func (v *Switchable) IsEnabled() bool {
	return v.Disabled == nil
}

func (v *Switchable) IsEnabledAt(at time.Time) bool {
	return v.Enabled != nil && v.Enabled.At.Time().Equal(at)
}

func (v *Switchable) IsDisabled() bool {
	return v.Disabled != nil
}

func (v *Switchable) IsDisabledAt(at time.Time) bool {
	return v.Disabled != nil && v.Disabled.At.Time().Equal(at)
}

// IsDisabledDirectly returns true if the entity has been disabled directly, not together with its parent.
func (v *Switchable) IsDisabledDirectly() bool {
	return v.Disabled != nil && v.Disabled.Directly
}

func (v *Switchable) DisabledBy() *By {
	if v.Disabled == nil {
		return nil
	}
	value := v.Disabled.By
	return &value
}

func (v *Switchable) DisabledAt() *utctime.UTCTime {
	if v.Disabled == nil {
		return nil
	}
	value := v.Disabled.At
	return &value
}

func (v *Switchable) DisabledReason() string {
	if v.Disabled == nil {
		return ""
	}
	return v.Disabled.Reason
}

func (v *Switchable) EnabledBy() *By {
	if v.Enabled == nil {
		return nil
	}
	value := v.Enabled.By
	return &value
}

func (v *Switchable) EnabledAt() *utctime.UTCTime {
	if v.Enabled == nil {
		return nil
	}
	value := v.Enabled.At
	return &value
}
