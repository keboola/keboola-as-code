package test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

func Versioned() definition.Versioned {
	now := utctime.MustParse("2000-01-01T01:00:00.000Z")
	return definition.Versioned{
		Created: definition.Created{At: now, By: ByUser()},
		Version: definition.Version{
			Number:      1,
			Hash:        "0123456789123456",
			ModifiedAt:  now,
			ModifiedBy:  ByUser(),
			Description: "foo bar",
		},
	}
}
