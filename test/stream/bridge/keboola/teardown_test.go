package keboola_test

import (
	"context"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (ts *testState) teardown(t *testing.T, ctx context.Context) {
	t.Helper()

	ts.logSection(t, "teardown")

	ts.shutdown(t, ctx, []withProcess{
		ts.apiScp,
		ts.sourceScp1,
		ts.sourceScp2,
	})

	ts.shutdown(t, ctx, []withProcess{
		ts.writerScp1,
		ts.writerScp2,
		ts.readerScp1,
		ts.readerScp2,
		ts.coordinatorScp1,
		ts.coordinatorScp2,
	})

	// No error is logged in the remaining logs
	ts.logger.AssertJSONMessages(t, "")

	ts.logSection(t, "teardown done")
}

func (ts *testState) shutdown(t *testing.T, ctx context.Context, scopes []withProcess) {
	t.Helper()

	for _, s := range scopes {
		s.Process().Shutdown(ctx, errors.New("bye bye"))
	}

	for _, s := range scopes {
		s.Process().WaitForShutdown()
	}
}
