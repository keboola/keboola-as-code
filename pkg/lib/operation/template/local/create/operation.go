package create

import (
	"context"
	"fmt"
	"io"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/create"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	createTemplateDir "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/dir/create"
	createTemplateInputs "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/inputs/create"
	saveInputs "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/inputs/save"
	createTemplateManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/manifest/create"
	saveRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/template/sync/pull"
)

type Options struct {
	ID           string
	Name         string
	Description  string
	SourceBranch model.BranchKey
	Configs      []create.ConfigDef
	StepsGroups  template.StepsGroups
	Components   []string
}

type dependencies interface {
	Components() *model.ComponentsMap
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	LocalTemplateRepository(ctx context.Context) (*repository.Repository, bool, error)
	Logger() log.Logger
	Template(ctx context.Context, reference model.TemplateRef) (*template.Template, error)
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.local.create")
	defer span.End(&err)

	// Get repository
	repo, _, err := d.LocalTemplateRepository(ctx)
	if err != nil {
		return err
	}
	manifest := repo.Manifest()

	// Get or create manifest record
	templateRecord := manifest.GetOrCreate(o.ID)

	// Set name and description
	templateRecord.Name = o.Name
	templateRecord.Description = o.Description

	// Get next major version
	version := model.ZeroSemVersion()
	if latest, found := templateRecord.DefaultVersion(); found {
		version = latest.Version.IncMajor()
	}

	// Init template directory
	versionRecord := templateRecord.AddVersion(version, o.Components)
	if _, err := createDir(ctx, o, d, repo.Fs(), templateRecord, versionRecord); err != nil {
		return err
	}

	// Save manifest
	manifest.Persist(templateRecord)
	if _, err := saveRepositoryManifest.Run(ctx, repo.Manifest(), repo.Fs(), d); err != nil {
		return err
	}

	// Template context
	templateCtx := create.NewContext(ctx, o.SourceBranch, o.Configs)

	// Template definition
	templateDef := model.NewTemplateRef(repo.Definition(), o.ID, versionRecord.Version.String())

	// Get template instance
	tmpl, err := d.Template(ctx, templateDef)
	if err != nil {
		return err
	}

	// Pull remote objects
	if err := pull.Run(ctx, tmpl, pull.Options{Context: templateCtx}, d); err != nil {
		return err
	}

	// Save inputs
	if err := saveInputs.Run(ctx, o.StepsGroups, tmpl.Fs(), d); err != nil {
		return err
	}

	// Done
	templatePath := filesystem.Join(templateRecord.Path, versionRecord.Path)
	d.Logger().Infof(ctx, `Template "%s" has been created.`, templatePath)

	return nil
}

func createDir(ctx context.Context, o Options, d dependencies, repositoryDir filesystem.Fs, templateRecord repositoryManifest.TemplateRecord, versionRecord repositoryManifest.VersionRecord) (filesystem.Fs, error) {
	// Create directory
	templatePath := filesystem.Join(templateRecord.Path, versionRecord.Path)
	fs, err := createTemplateDir.Run(ctx, repositoryDir, createTemplateDir.Options{Path: templatePath}, d)
	if err != nil {
		return nil, err
	}

	// Create src dir
	if err := fs.Mkdir(ctx, template.SrcDirectory); err != nil {
		return nil, err
	}

	// Create tests dir + .gitkeep
	gitKeepFile := filesystem.
		NewRawFile(filesystem.Join(template.TestsDirectory, ".gitkeep"), "\n").
		AddTag(model.FileKindGitKeep).
		AddTag(model.FileTypeOther)
	if err := fs.WriteFile(ctx, gitKeepFile); err != nil {
		return nil, err
	}

	// Create files
	if _, err := createTemplateManifest.Run(ctx, fs, d); err != nil {
		return nil, err
	}
	if _, err := createTemplateInputs.Run(ctx, fs, d); err != nil {
		return nil, err
	}
	if err := createLongDesc(ctx, o, d, fs); err != nil {
		return nil, err
	}
	if err := createReadme(ctx, o, d, fs); err != nil {
		return nil, err
	}
	return fs, nil
}

func createLongDesc(ctx context.Context, o Options, d dependencies, fs filesystem.Fs) error {
	content := "### %s\n\n%s\n\n"
	path := filesystem.Join("src", template.LongDescriptionFile)
	file := filesystem.NewRawFile(path, fmt.Sprintf(content, o.Name, `Extended description`)).SetDescription(`extended description`)
	if err := fs.WriteFile(ctx, file); err != nil {
		return err
	}
	d.Logger().Infof(ctx, "Created extended description file \"%s\".", file.Path())
	return nil
}

func createReadme(ctx context.Context, o Options, d dependencies, fs filesystem.Fs) error {
	content := "### %s\n\n%s\n\n"
	path := filesystem.Join("src", template.ReadmeFile)
	file := filesystem.NewRawFile(path, fmt.Sprintf(content, o.Name, o.Description)).SetDescription(`readme`)
	if err := fs.WriteFile(ctx, file); err != nil {
		return err
	}
	d.Logger().Infof(ctx, "Created readme file \"%s\".", file.Path())
	return nil
}
