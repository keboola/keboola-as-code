package testhelper

import (
	"context"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/request"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func WaitForAPI(ctx context.Context, cmdWaitCh <-chan error, nodeID string, baseURL string, startupTimeout time.Duration) error {
	c := client.NewTestClient().WithBaseURL(baseURL)
	timeout := time.After(startupTimeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	// Keep trying until time out or got a result or got an error
	for {
		select {
		// Handle timeout
		case <-timeout:
			return errors.Errorf(`node "%s" didn't start within %s`, nodeID, startupTimeout)
		// Check process
		case err := <-cmdWaitCh:
			return errors.Errorf(`node "%s" was terminated unexpectedly: %w`, nodeID, err)
		// Periodically test health check endpoint
		case <-ticker.C:
			resp, _, err := request.NewHTTPRequest(c).WithGet("/health-check").Send(ctx)
			if err != nil && strings.Contains(err.Error(), "connection refused") {
				continue
			} else if err != nil {
				return err
			} else if resp.StatusCode() == 200 {
				return nil
			}
		}
	}
}
