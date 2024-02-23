package definition

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type SwitchableInterface interface {
	Enable()
	Disable(now time.Time, by, reason string)
	IsEnabled() bool
	GetDisabledBy() string
	GetDisabledAt() *utctime.UTCTime
	GetDisabledReason() string
}

type Switchable struct {
	Disabled       bool             `json:"disabled,omitempty"`
	DisabledBy     string           `json:"disabledBy,omitempty" validate:"required_if=Disabled true,excluded_if=Disabled false"`
	DisabledAt     *utctime.UTCTime `json:"disabledAt,omitempty"  validate:"required_if=Disabled true,excluded_if=Disabled false"`
	DisabledReason string           `json:"disabledReason,omitempty"  validate:"required_if=Disabled true,excluded_if=Disabled false"`
}

func (v *Switchable) IsEnabled() bool {
	return !v.Disabled
}

func (v *Switchable) Enable() {
	v.Disabled = false
	v.DisabledBy = ""
	v.DisabledAt = nil
	v.DisabledReason = ""
}

func (v *Switchable) Disable(now time.Time, by, reason string) {
	at := utctime.From(now)
	v.Disabled = true
	v.DisabledBy = by
	v.DisabledAt = &at
	v.DisabledReason = reason
}

func (v *Switchable) GetDisabledBy() string {
	return v.DisabledBy
}

func (v *Switchable) GetDisabledAt() *utctime.UTCTime {
	return v.DisabledAt
}

func (v *Switchable) GetDisabledReason() string {
	return v.DisabledReason
}
