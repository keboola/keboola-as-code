package version

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/semver"

	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

const EnvVersionCheck = "KBC_VERSION_CHECK"

type checker struct {
	ctx    context.Context
	envs   *env.Map
	client client.Client
	cancel context.CancelFunc
	logger log.Logger
}

func NewGitHubChecker(parentCtx context.Context, logger log.Logger, envs *env.Map) *checker {
	// Timeout 3 seconds
	ctx, cancel := context.WithTimeout(parentCtx, 3*time.Second)

	// Create client
	c := client.New().WithBaseURL("https://api.github.com")
	return &checker{ctx, envs, c, cancel, logger}
}

func (c *checker) CheckIfLatest(currentVersion string) error {
	defer c.cancel()

	// Dev build
	if currentVersion == DevVersionValue {
		return fmt.Errorf(`skipped, found dev build`)
	}

	// Disabled by ENV
	if value, _ := c.envs.Lookup(EnvVersionCheck); strings.ToLower(value) == "false" {
		return fmt.Errorf(fmt.Sprintf(`skipped, disabled by ENV "%s"`, EnvVersionCheck))
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
		c.logger.Warn(`*******************************************************`)
		c.logger.Warnf(`WARNING: A new version "%s" is available.`, latestVersion)
		c.logger.Warnf(`You are currently using version "%s".`, current.String())
		c.logger.Warn(`Please update to get the latest features and bug fixes.`)
		c.logger.Warn(`Read more: https://github.com/keboola/keboola-as-code/releases`)
		c.logger.Warn(`*******************************************************`)
		c.logger.Warn()
	}

	return nil
}

func (c *checker) getLatestVersion() (string, error) {
	// Load releases
	// The last release may be without assets (build in progress), so we load the last 5 releases.
	result := make([]interface{}, 0)
	_, _, err := client.
		NewHTTPRequest().
		WithGet("repos/keboola/keboola-as-code/releases?per_page=5").
		WithResult(&result).
		Send(c.ctx, c.client)
	if err != nil {
		return "", err
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

	return "", fmt.Errorf(`failed to parse the latest version`)
}
