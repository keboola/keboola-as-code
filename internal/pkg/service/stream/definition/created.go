package definition

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type CreatedInterface interface {
	CreatedAt() utctime.UTCTime
	CreatedBy() By
}

type Created struct {
	Created CreatedData `json:"created"`
}

type CreatedData struct {
	At utctime.UTCTime `json:"at" hash:"ignore" validate:"required"`
	By By              `json:"by" hash:"ignore" validate:"required"`
}

func (v *Created) SetCreation(at time.Time, by By) {
	if v.Created.At.IsZero() {
		v.Created.At = utctime.From(at)
		v.Created.By = by
	}
}

func (v *Created) CreatedAt() utctime.UTCTime {
	return v.Created.At
}

func (v *Created) CreatedBy() By {
	return v.Created.By
}
