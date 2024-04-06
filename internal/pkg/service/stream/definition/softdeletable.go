package definition

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type SoftDeletableInterface interface {
	// Delete marks the entity as deleted.
	Delete(now time.Time, by By, directly bool)
	// Undelete marks the entity as undeleted.
	Undelete(now time.Time, by By)
	// IsDeleted returns true if the entity is marked as deleted.
	IsDeleted() bool
	// IsDeletedDirectly returns true if the entity has been deleted directly, not together with its parent.
	IsDeletedDirectly() bool
	EntityDeletedBy() *By
	EntityDeletedAt() *utctime.UTCTime
	EntityUndeletedBy() *By
	EntityUndeletedAt() *utctime.UTCTime
}

type SoftDeletable struct {
	Deleted   *Deleted   `json:"deleted,omitempty" validate:"excluded_with=Undeleted"`
	Undeleted *Undeleted `json:"undeleted,omitempty" validate:"excluded_with=Deleted"`
}

type Deleted struct {
	// Directly is true if the entity has been deleted directly, not together with its parent.
	Directly bool            `json:"directly"`
	At       utctime.UTCTime `json:"at" validate:"required"`
	By       By              `json:"by" validate:"required"`
}

type Undeleted struct {
	At utctime.UTCTime `json:"at" validate:"required"`
	By By              `json:"by" validate:"required"`
}

func (v *SoftDeletable) Delete(now time.Time, by By, directly bool) {
	v.Deleted = &Deleted{By: by, At: utctime.From(now), Directly: directly}
	v.Undeleted = nil

}

func (v *SoftDeletable) Undelete(now time.Time, by By) {
	v.Deleted = nil
	v.Undeleted = &Undeleted{By: by, At: utctime.From(now)}
}

func (v *SoftDeletable) IsDeleted() bool {
	return v.Deleted != nil
}

// IsDeletedDirectly returns true if the object has been deleted directly, not together with its parent.
func (v *SoftDeletable) IsDeletedDirectly() bool {
	return v.Deleted != nil && v.Deleted.Directly
}

func (v *SoftDeletable) EntityDeletedBy() *By {
	if v.Deleted == nil {
		return nil
	}
	value := v.Deleted.By
	return &value
}

func (v *SoftDeletable) EntityDeletedAt() *utctime.UTCTime {
	if v.Deleted == nil {
		return nil
	}
	value := v.Deleted.At
	return &value
}

func (v *SoftDeletable) EntityUndeletedBy() *By {
	if v.Undeleted == nil {
		return nil
	}
	value := v.Undeleted.By
	return &value
}

func (v *SoftDeletable) EntityUndeletedAt() *utctime.UTCTime {
	if v.Undeleted == nil {
		return nil
	}
	value := v.Undeleted.At
	return &value
}
