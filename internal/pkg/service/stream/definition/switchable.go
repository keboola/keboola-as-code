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
	// IsDisabled returns true if the entity is marked as disabled.
	IsDisabled() bool
	// IsDisabledDirectly returns true if the entity has been disabled directly, not together with its parent.
	IsDisabledDirectly() bool
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

func (v *Switchable) IsDisabled() bool {
	return v.Disabled != nil
}

// IsDisabledDirectly returns true if the entity has been disabled directly, not together with its parent.
func (v *Switchable) IsDisabledDirectly() bool {
	return v.Disabled != nil && v.Disabled.Directly
}

func (v *Switchable) EntityDisabledBy() *By {
	if v.Disabled == nil {
		return nil
	}
	value := v.Disabled.By
	return &value
}

func (v *Switchable) EntityDisabledAt() *utctime.UTCTime {
	if v.Disabled == nil {
		return nil
	}
	value := v.Disabled.At
	return &value
}

func (v *Switchable) EntityDisabledReason() string {
	if v.Disabled == nil {
		return ""
	}
	return v.Disabled.Reason
}

func (v *Switchable) EntityEnabledBy() *By {
	if v.Enabled == nil {
		return nil
	}
	value := v.Enabled.By
	return &value
}

func (v *Switchable) EntityEnabledAt() *utctime.UTCTime {
	if v.Enabled == nil {
		return nil
	}
	value := v.Enabled.At
	return &value
}
