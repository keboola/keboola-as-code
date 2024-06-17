package test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

func Created() definition.Created {
	now := utctime.MustParse("2000-01-01T01:00:00.000Z")
	return definition.Created{Created: definition.CreatedData{At: now, By: ByUser()}}
}
