package plugin

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

func isDeletedNow(now time.Time, updated definition.SoftDeletableInterface) bool {
	return updated.DeletedAt().Time().Equal(now)
}

func isUndeletedNow(now time.Time, updated definition.SoftDeletableInterface) bool {
	return updated.UndeletedAt().Time().Equal(now)
}

func isActivatedNow(now time.Time, updated definition.SwitchableInterface) bool {
	var created bool
	if v, ok := updated.(definition.CreatedInterface); ok {
		created = v.CreatedAt().Time().Equal(now)
	}

	var undeleted bool
	if v, ok := updated.(definition.SoftDeletableInterface); ok {
		undeleted = isUndeletedNow(now, v)
	}

	var enabled bool
	{
		at := updated.EnabledAt()
		enabled = at != nil && at.Time().Equal(now)
	}

	return created || undeleted || enabled
}

func isDeactivatedNow(now time.Time, updated definition.SwitchableInterface) bool {
	var deleted bool
	if v, ok := updated.(definition.SoftDeletableInterface); ok {
		deleted = isDeletedNow(now, v)
	}

	var disabled bool
	{
		at := updated.DisabledAt()
		disabled = at != nil && at.Time().Equal(now)
	}

	return deleted || disabled
}
