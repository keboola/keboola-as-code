package keboola_test

import (
	"context"
	"math/rand"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (ts *testState) teardown(t *testing.T, ctx context.Context) {
	t.Helper()

	ts.logSection(t, "teardown")

	nodes := []withProcess{
		ts.apiScp,
		ts.writerScp1,
		ts.writerScp2,
		ts.readerScp1,
		ts.readerScp2,
		ts.coordinatorScp1,
		ts.coordinatorScp2,
		ts.sourceScp1,
		ts.sourceScp2,
	}

	// Shutdown must work always, in random other
	rand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})

	ts.shutdown(t, ctx, nodes)

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
