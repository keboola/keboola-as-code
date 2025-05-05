package version

import (
	"context"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/request"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type checker struct {
	ctx    context.Context
	client client.Client
	cancel context.CancelFunc
	logger log.Logger
	skip   bool
}

func NewGitHubChecker(parentCtx context.Context, logger log.Logger, skip bool) *checker {
	// Timeout 3 seconds
	ctx, cancel := context.WithTimeoutCause(parentCtx, 3*time.Second, errors.New("github check timeout"))

	// Create client
	c := client.New().WithBaseURL("https://api.github.com")
	return &checker{ctx: ctx, client: c, cancel: cancel, logger: logger, skip: skip}
}

func (c *checker) CheckIfLatest(ctx context.Context, currentVersion string) error {
	defer c.cancel()

	// Dev build
	if currentVersion == DevVersionValue {
		return errors.New(`skipped, found dev build`)
	}

	// Disabled by ENV
	if c.skip {
		return errors.New(`skipped, check disabled`)
	}

	latestVersion, err := c.getLatestVersion()
	if err != nil {
		return err
	}

	current, err := semver.NewVersion(currentVersion)
	if err != nil {
		return err
	}

	latest, err := semver.NewVersion(latestVersion)
	if err != nil {
		return err
	}

	if latest.GreaterThan(current) {
		c.logger.Warn(ctx, `*******************************************************`)
		c.logger.Warnf(ctx, `WARNING: A new version "%s" is available.`, latestVersion)
		c.logger.Warnf(ctx, `You are currently using version "%s".`, current.String())
		c.logger.Warn(ctx, `Please update to get the latest features and bug fixes.`)
		c.logger.Warn(ctx, `Read more: https://github.com/keboola/keboola-as-code/releases`)
		c.logger.Warn(ctx, `*******************************************************`)
		c.logger.Warn(ctx, "")
	}

	return nil
}

func (c *checker) getLatestVersion() (string, error) {
	// Load releases
	// The last release may be without assets (build in progress), so we load the last 5 releases.
	result := make([]any, 0)
	_, _, err := request.NewHTTPRequest(c.client).
		WithGet("repos/keboola/keboola-as-code/releases?per_page=5").
		WithResult(&result).
		Send(c.ctx)
	if err != nil {
		return "", err
	}

	// Determine the latest version
	for _, item := range result {
		// Release is object
		release, ok := item.(map[string]any)
		if !ok {
			continue
		}

		// Contains assets key
		assetsRaw, ok := release["assets"]
		if !ok {
			continue
		}

		// Assets is an array
		assets, ok := assetsRaw.([]any)
		if !ok {
			continue
		}

		// Skip empty assets
		if len(assets) == 0 {
			continue
		}

		// Release contains tag_name
		nameRaw, ok := release["tag_name"]
		if !ok {
			continue
		}

		// Tag name is string
		name, ok := nameRaw.(string)
		if !ok {
			continue
		}

		// Skip draft
		if release["draft"] == true {
			continue
		}

		// Skip pre-release
		if release["prerelease"] == true {
			continue
		}

		// Ok, name found
		if name != "" {
			return name, nil
		}
	}

	return "", errors.New(`failed to parse the latest version`)
}
