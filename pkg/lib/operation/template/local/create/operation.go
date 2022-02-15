package create

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	createTemplateDir "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/dir/create"
	createTemplateInputs "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/inputs/create"
	saveInputs "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/inputs/save"
	createTemplateManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/manifest/create"
	saveRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/save"
	loadStateOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/state/load"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/template/sync/pull"
)

type Options struct {
	Id           string
	Name         string
	Description  string
	SourceBranch model.BranchKey
	Configs      []template.ConfigDef
	Inputs       *template.Inputs
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	LocalTemplateRepository() (*repository.Repository, error)
	Template(reference model.TemplateRef) (*template.Template, error)
	TemplateState(options loadStateOp.Options) (*template.State, error)
}

func Run(o Options, d dependencies) (err error) {
	// Get repository
	repo, err := d.LocalTemplateRepository()
	if err != nil {
		return err
	}
	manifest := repo.Manifest()

	// Get or create manifest record
	templateRecord := manifest.GetOrCreate(o.Id)

	// Set name and description
	templateRecord.Name = o.Name
	templateRecord.Description = o.Description

	// Get next major version
	var version model.SemVersion
	if latest, found := templateRecord.LatestVersion(); found {
		version = latest.Version.IncMajor()
	} else {
		version = model.ZeroSemVersion()
	}

	// Init template directory
	versionRecord := templateRecord.AddVersion(version)
	if err := initTemplateDir(o, d, repo.Fs(), versionRecord); err != nil {
		return err
	}

	// Save manifest
	manifest.Persist(templateRecord)
	if _, err := saveRepositoryManifest.Run(repo.Manifest(), repo.Fs(), d); err != nil {
		return err
	}

	// Template definition
	templateDef := model.NewTemplateRef(model.TemplateRepositoryWorkingDir(), o.Id, versionRecord.Version)

	// Template context
	templateCtx := template.NewCreateContext(d.Ctx(), o.SourceBranch, o.Configs)

	// Get template instance
	tmpl, err := d.Template(templateDef)
	if err != nil {
		return err
	}

	// Pull remote objects
	if err := pull.Run(pull.Options{Template: tmpl, Context: templateCtx}, d); err != nil {
		return err
	}

	// Save inputs
	if err := saveInputs.Run(o.Inputs, tmpl.Fs(), d); err != nil {
		return err
	}

	// Done
	d.Logger().Infof(`Template "%s" has been created.`, versionRecord.Path())

	return nil
}

func initTemplateDir(o Options, d dependencies, repositoryDir filesystem.Fs, record repositoryManifest.VersionRecord) error {
	// Create directory
	fs, err := createTemplateDir.Run(createTemplateDir.Options{RepositoryDir: repositoryDir, Path: record.Path()}, d)
	if err != nil {
		return err
	}

	// Create src dir
	if err := fs.Mkdir(template.SrcDirectory); err != nil {
		return err
	}

	// Create tests dir + .gitkeep
	gitKeepFile := filesystem.
		NewRawFile(filesystem.Join(template.TestsDirectory, ".gitkeep"), "\n").
		AddTag(model.FileKindGitKeep).
		AddTag(model.FileTypeOther)
	if err := fs.WriteFile(gitKeepFile); err != nil {
		return err
	}

	// Create files
	if _, err := createTemplateManifest.Run(fs, d); err != nil {
		return err
	}
	if _, err := createTemplateInputs.Run(fs, d); err != nil {
		return err
	}
	if err := createReadme(o, d, fs); err != nil {
		return err
	}
	return nil
}

func createReadme(o Options, d dependencies, fs filesystem.Fs) error {
	content := "### %s\n\n%s\n\n"
	file := filesystem.NewRawFile(`README.md`, fmt.Sprintf(content, o.Name, o.Description)).SetDescription(`readme`)
	if err := fs.WriteFile(file); err != nil {
		return err
	}
	d.Logger().Infof("Created readme file \"%s\".", file.Path())
	return nil
}
