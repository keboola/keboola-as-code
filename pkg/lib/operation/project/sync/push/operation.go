package push

import (
	"context"
	"io"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/plan/push"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/encrypt"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/validate"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/create"
)

const KBCIgnoreFilePath = ".keboola/kbc_ignore"

type Options struct {
	Encrypt           bool
	DryRun            bool
	SkipValidation    bool
	AllowRemoteDelete bool
	LogUntrackedPaths bool
	ChangeDescription string
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	ProjectID() keboola.ProjectID
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.sync.push")
	defer span.End(&err)

	logger := d.Logger()

	// Encrypt before push?
	if o.Encrypt {
		if err := encrypt.Run(ctx, projectState, encrypt.Options{DryRun: o.DryRun, LogEmpty: true}, d); err != nil {
			return err
		}
	}

	// Change description - optional arg
	logger.Debugf(ctx, `Change description: "%s"`, o.ChangeDescription)

	// Log untracked paths
	if o.LogUntrackedPaths {
		projectState.LogUntrackedPaths(ctx, logger)
	}

	// Validate
	if !o.SkipValidation {
		validateOptions := validate.Options{
			ValidateSecrets:    !o.Encrypt || !o.DryRun,
			ValidateJSONSchema: true,
		}
		if err := validate.Run(ctx, projectState, validateOptions, d); err != nil {
			return err
		}
	}

	// Check if the KBC ignore file exists in the project state and the AllowRemoteDelete option is disabled.
	// If the ignore file is present, read its content. If an error occurs while reading, return the error.
	// If the content is non-empty, apply the ignored configurations to the project state.
	if projectState.Fs().Exists(ctx, KBCIgnoreFilePath) && !o.AllowRemoteDelete {
		err := setIgnoredConfigsOrRows(ctx, projectState.Registry, projectState.Fs(), KBCIgnoreFilePath)
		if err != nil {
			return err
		}
		ignoreConfigsAndRows(projectState.Registry)
	}

	// Diff
	results, err := createDiff.Run(ctx, createDiff.Options{Objects: projectState}, d, diff.WithIgnoreBranchName(projectState.ProjectManifest().AllowTargetENV()))
	if err != nil {
		return err
	}

	// Get plan
	plan, err := push.NewPlan(results, projectState.ProjectManifest().AllowTargetENV())
	if err != nil {
		return err
	}

	// Allow remote deletion, if --force
	if o.AllowRemoteDelete {
		plan.AllowRemoteDelete()
	}

	// Log plan
	plan.Log(d.Stdout())

	if !plan.Empty() {
		// Dry run?
		if o.DryRun {
			logger.Info(ctx, "Dry run, nothing changed.")
			return nil
		}

		// Invoke
		if err := plan.Invoke(logger, ctx, projectState.LocalManager(), projectState.RemoteManager(), o.ChangeDescription); err != nil {
			return err
		}

		logger.Info(ctx, "Push done.")
	}
	return nil
}

func parseIgnorePatterns(content string) []string {
	var ignorePatterns []string
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)
		// Skip empty lines and comments
		if trimmedLine != "" && !strings.HasPrefix(trimmedLine, "#") {
			ignorePatterns = append(ignorePatterns, trimmedLine)
		}
	}

	return ignorePatterns
}

func ignoreConfigsAndRows(projectState *project.State) {
	if len(projectState.IgnoredConfigs()) > 0 {
		for _, v := range projectState.IgnoredConfigs() {
			v.SetLocalState(nil)
		}
	}

	if len(projectState.IgnoredConfigRows()) > 0 {
		for _, v := range projectState.IgnoredConfigRows() {
			v.SetLocalState(nil)
		}
	}
}

func setIgnoredConfigsOrRows(ctx context.Context, projectState *project.State) error {
	content, err := projectState.Fs().ReadFile(ctx, filesystem.NewFileDef(KBCIgnoreFilePath))
	if err != nil {
		return err
	}
	for _, val := range parseIgnorePatterns(content.Content) {
		ignoredConfigOrRows := strings.Split(val, "/")

		switch len(ignoredConfigOrRows) {
		case 3:
			projectState.IgnoreConfigRow(ignoredConfigOrRows[2])
		case 2:
			projectState.IgnoreConfig(ignoredConfigOrRows[1])
		}
	}

	ignoreConfigsAndRows(projectState)
	return nil
}
