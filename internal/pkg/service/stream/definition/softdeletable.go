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
	IsDeletedAt(at time.Time) bool
	// IsUndeleted returns true if the entity is marked as undeleted.
	IsUndeleted() bool
	IsUndeletedAt(at time.Time) bool
	// IsDeletedDirectly returns true if the entity has been deleted directly, not together with its parent.
	IsDeletedDirectly() bool
	DeletedBy() *By
	DeletedAt() utctime.UTCTime
	UndeletedBy() *By
	UndeletedAt() utctime.UTCTime
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

func (v *SoftDeletable) IsDeletedAt(at time.Time) bool {
	return v.Deleted != nil && v.Deleted.At.Time().Equal(at)
}

// IsDeletedDirectly returns true if the object has been deleted directly, not together with its parent.
func (v *SoftDeletable) IsDeletedDirectly() bool {
	return v.Deleted != nil && v.Deleted.Directly
}

func (v *SoftDeletable) DeletedBy() *By {
	if v.Deleted == nil {
		return nil
	}
	value := v.Deleted.By
	return &value
}

func (v *SoftDeletable) DeletedAt() utctime.UTCTime {
	if v.Deleted == nil {
		return utctime.UTCTime{}
	}
	return v.Deleted.At
}

func (v *SoftDeletable) IsUndeleted() bool {
	return v.Undeleted != nil
}

func (v *SoftDeletable) IsUndeletedAt(at time.Time) bool {
	return v.Undeleted != nil && v.Undeleted.At.Time().Equal(at)
}

func (v *SoftDeletable) UndeletedBy() *By {
	if v.Undeleted == nil {
		return nil
	}
	value := v.Undeleted.By
	return &value
}

func (v *SoftDeletable) UndeletedAt() utctime.UTCTime {
	if v.Undeleted == nil {
		return utctime.UTCTime{}
	}
	return v.Undeleted.At
}
