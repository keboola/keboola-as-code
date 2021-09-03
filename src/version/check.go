package version

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"keboola-as-code/src/client"
)

type checker struct {
	api    *client.Client
	cancel context.CancelFunc
	logger *zap.SugaredLogger
}

func NewChecker(parentCtx context.Context, logger *zap.SugaredLogger) *checker {
	// Timeout 3 seconds
	ctx, cancel := context.WithTimeout(parentCtx, 3*time.Second)

	// Create client
	api := client.NewClient(ctx, logger, false).WithHostUrl(`https://api.github.com`)
	return &checker{api, cancel, logger}
}

func (c *checker) CheckIfLatest(currentVersion string) error {
	defer c.cancel()

	if currentVersion == DevVersionValue {
		return fmt.Errorf(`skipped, found dev build`)
	}

	latestVersion, err := c.getLatestVersion()
	if err != nil {
		return err
	}

	if currentVersion != latestVersion {
		c.logger.Warn(`*******************************************************`)
		c.logger.Warnf(`WARNING: A new version "%s" is available.`, latestVersion)
		c.logger.Warn(`Please update to get the latest features and bug fixes.`)
		c.logger.Warn(`*******************************************************`)
		c.logger.Warn()
	}

	return nil
}

func (c *checker) getLatestVersion() (string, error) {
	// Load releases
	// The last release may be without assets (build in progress), so we load the last 5 releases.
	result := make([]interface{}, 0)
	releases := c.api.
		NewRequest(`GET`, `repos/keboola/keboola-as-code/releases?per_page=5`).
		SetResult(&result).
		Send().
		Response
	if releases.HasError() {
		return "", releases.Err()
	}

	// Determine the latest version
	for _, item := range result {
		// Release is object
		release, ok := item.(map[string]interface{})
		if !ok {
			continue
		}

		// Contains assets key
		assetsRaw, ok := release["assets"]
		if !ok {
			continue
		}

		// Assets is an array
		assets, ok := assetsRaw.([]interface{})
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

		// Ok, name found
		if name != "" {
			return name, nil
		}
	}

	return "", fmt.Errorf(`failed to parse the latest version`)
}
