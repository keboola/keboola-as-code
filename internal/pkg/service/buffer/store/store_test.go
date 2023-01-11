package store

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func newStoreForTest(t *testing.T) *Store {
	t.Helper()
	now, _ := time.Parse(time.RFC3339, "2010-01-01T01:01:01+07:00")
	clk := clock.NewMock()
	clk.Set(now)
	return newFrom(clk, log.NewNopLogger(), telemetry.NewNopTracer(), etcdhelper.ClientForTest(t), schema.New(validator.New().Validate))
}
