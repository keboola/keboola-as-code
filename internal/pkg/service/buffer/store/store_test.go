package store

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func newStoreForTest(t *testing.T) *Store {
	t.Helper()
	return New(log.NewNopLogger(), etcdhelper.ClientForTest(t), telemetry.NewNopTracer())
}
