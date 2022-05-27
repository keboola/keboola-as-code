package client

import (
	"context"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type MockedClientOption func() {

}

func NewMockedClient(t *testing.T, verbose bool) (*Client, *httpmock.MockTransport, log.DebugLogger) {
	t.Helper()

	// Create
	logger := log.NewDebugLogger()
	c := NewClient(context.Background(), logger, verbose)

	// Set short retry delay in tests
	c.resty.RetryWaitTime = 1 * time.Millisecond
	c.resty.RetryMaxWaitTime = 1 * time.Millisecond

	// Mocked resty transport
	transport := httpmock.NewMockTransport()
	c.resty.GetClient().Transport = transport
	return c, transport, logger
}
