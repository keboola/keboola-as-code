package workflows

import (
	"bufio"
	"bytes"
	"context"
	"embed"
	"text/template"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// nolint:gochecknoglobals
//
//go:embed template/*
var templates embed.FS

type Options struct {
	Validate   bool // validate all branches
	Push       bool // push to Keboola Connection state in the main branch
	Pull       bool // periodical pull new changes to the main branch
	MainBranch string
}

func (o *Options) Enabled() bool {
	return o.Validate || o.Push || o.Pull
}

type generator struct {
	fs      filesystem.Fs
	options *Options
	logger  log.Logger
	errors  errors.MultiError
}

func GenerateFiles(ctx context.Context, logger log.Logger, fs filesystem.Fs, options *Options) error {
	g := &generator{fs: fs, options: options, logger: logger, errors: errors.NewMultiError()}
	return g.generateFiles(ctx)
}

func (g *generator) generateFiles(ctx context.Context) error {
	if !g.options.Enabled() {
		g.logger.Info(ctx, "")
		g.logger.Info(ctx, "No continuous integration action selected.")
		return nil
	}

	// Common files
	g.logger.Info(ctx, "")
	g.logger.Info(ctx, `Generating CI workflows ...`)
	workflowsDir := filesystem.Join(".github", "workflows")
	actionsDir := filesystem.Join(".github", "actions")
	installActDir := filesystem.Join(actionsDir, "install")
	g.handleError(g.fs.Mkdir(ctx, workflowsDir))
	g.handleError(g.fs.Mkdir(ctx, installActDir))
	g.renderTemplate(ctx, `template/install.yml.tmpl`, filesystem.Join(installActDir, `action.yml`))

	// Validate operation
	if g.options.Validate {
		g.renderTemplate(ctx, `template/validate.yml.tmpl`, filesystem.Join(workflowsDir, `validate.yml`))
	}

	// Push operation
	if g.options.Push {
		g.renderTemplate(ctx, `template/push.yml.tmpl`, filesystem.Join(workflowsDir, `push.yml`))
	}

	// Pull operation
	if g.options.Pull {
		g.renderTemplate(ctx, `template/pull.yml.tmpl`, filesystem.Join(workflowsDir, `pull.yml`))
	}

	if g.errors.Len() > 0 {
		return g.errors
	}

	g.logger.Info(ctx, "")
	g.logger.Info(ctx, "CI workflows have been generated.")
	g.logger.Info(ctx, "Feel free to modify them.")
	g.logger.Info(ctx, "")
	g.logger.Info(ctx, "Please set the secret KBC_MASTER_TOKEN in the GitHub settings.")
	g.logger.Info(ctx, "See: https://docs.github.com/en/actions/reference/encrypted-secrets")
	return nil
}

func (g *generator) handleError(err error) {
	if err != nil {
		g.errors.Append(err)
	}
}

func (g *generator) renderTemplate(ctx context.Context, templatePath, targetPath string) {
	// Load
	content, err := templates.ReadFile(templatePath)
	if err != nil {
		panic(err)
	}

	// Parse
	tmpl, err := template.New("test").Parse(string(content))
	if err != nil {
		panic(err)
	}

	// Render
	var buffer bytes.Buffer
	writer := bufio.NewWriter(&buffer)
	if err := tmpl.Execute(writer, g.options); err != nil {
		panic(err)
	}

	// Flush to buffer
	if err := writer.Flush(); err != nil {
		panic(err)
	}

	// Write
	if err := g.fs.WriteFile(ctx, filesystem.NewRawFile(targetPath, buffer.String())); err == nil {
		g.logger.Infof(ctx, `Created file "%s".`, targetPath)
	} else {
		g.errors.Append(err)
	}
}
