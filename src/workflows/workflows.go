package workflows

import (
	"bufio"
	"bytes"
	"embed"
	"os"
	"path/filepath"
	"text/template"

	"go.uber.org/zap"

	"keboola-as-code/src/utils"
)

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
	projectDir string
	options    *Options
	logger     *zap.SugaredLogger
	errors     *utils.Error
}

func GenerateFiles(logger *zap.SugaredLogger, projectDir string, options *Options) error {
	g := &generator{projectDir: projectDir, options: options, logger: logger, errors: utils.NewMultiError()}
	return g.generateFiles()
}

func (g *generator) generateFiles() error {
	if !g.options.Enabled() {
		g.logger.Info("")
		g.logger.Info("No continuous integration action selected.")
		return nil
	}

	// Common files
	g.logger.Info()
	g.logger.Info(`Generating CI workflows ...`)
	workflowsDir := filepath.Join(g.projectDir, ".github", "workflows")
	actionsDir := filepath.Join(g.projectDir, ".github", "actions")
	installActDir := filepath.Join(actionsDir, "install")
	g.handleError(os.MkdirAll(workflowsDir, 0755))
	g.handleError(os.MkdirAll(installActDir, 0755))
	g.renderTemplate(`template/install.yml.tmpl`, filepath.Join(installActDir, `action.yml`))

	// Validate operation
	if g.options.Validate {
		g.renderTemplate(`template/validate.yml.tmpl`, filepath.Join(workflowsDir, `validate.yml`))
	}

	// Push operation
	if g.options.Push {
		g.renderTemplate(`template/push.yml.tmpl`, filepath.Join(workflowsDir, `push.yml`))
	}

	// Pull operation
	if g.options.Pull {
		g.renderTemplate(`template/pull.yml.tmpl`, filepath.Join(workflowsDir, `pull.yml`))
	}

	if g.errors.Len() > 0 {
		return g.errors
	}

	g.logger.Info("")
	g.logger.Info("CI workflows have been generated.")
	g.logger.Info("Feel free to modify them.")
	g.logger.Info("")
	g.logger.Info("Please set the secret KBC_STORAGE_API_TOKEN in the GitHub settings.")
	g.logger.Info("See: https://docs.github.com/en/actions/reference/encrypted-secrets")
	return nil
}

func (g *generator) handleError(err error) {
	if err != nil {
		g.errors.Append(err)
	}
}

func (g *generator) renderTemplate(templatePath, targetPath string) {
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
	if err := os.WriteFile(targetPath, buffer.Bytes(), 0644); err == nil {
		g.logger.Infof(`Created file "%s".`, utils.RelPath(g.projectDir, targetPath))
	} else {
		g.errors.Append(err)
	}
}
