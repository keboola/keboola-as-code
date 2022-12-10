package store

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func newStoreForTest(t *testing.T) *Store {
	t.Helper()
	return New(log.NewNopLogger(), etcdhelper.ClientForTest(t), telemetry.NewNopTracer(), schema.New(validator.New().Validate))
}
