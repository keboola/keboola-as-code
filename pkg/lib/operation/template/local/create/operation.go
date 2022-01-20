package create

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	saveRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/save"
)

type ConfigDef struct {
	Key        model.ConfigKey
	TemplateId string
	Rows       []ConfigRowDef
}

type ConfigRowDef struct {
	Key        model.ConfigRowKey
	TemplateId string
}

type Options struct {
	Id          string
	Name        string
	Description string
	Configs     []ConfigDef
}

type dependencies interface {
	Logger() log.Logger
	TemplateRepositoryDir() (filesystem.Fs, error)
	TemplateRepositoryManifest() (*repositoryManifest.Manifest, error)
	CreateTemplateDir(path string) (filesystem.Fs, error)
	CreateTemplateManifest() (*templateManifest.Manifest, error)
	CreateTemplateInputs() (*template.Inputs, error)
}

func Run(o Options, d dependencies) (err error) {
	// Get dependencies
	manifest, err := d.TemplateRepositoryManifest()
	if err != nil {
		return err
	}

	// Get or create manifest record
	templateRecord := manifest.GetOrCreate(o.Id)

	// Set name and description
	templateRecord.Name = o.Name
	templateRecord.Description = o.Description

	// Get next major version
	var version template.Version
	if latest, found := templateRecord.LatestVersion(); found {
		version = latest.Version.IncMajor()
	} else {
		version = template.ZeroVersion()
	}

	// Init template directory
	versionRecord := templateRecord.AddVersion(version)
	if err := initTemplateDir(o, d, versionRecord); err != nil {
		return err
	}

	// Save manifest
	manifest.Persist(templateRecord)
	if _, err := saveRepositoryManifest.Run(d); err != nil {
		return err
	}

	return nil
}

func initTemplateDir(o Options, d dependencies, record repositoryManifest.VersionRecord) (err error) {
	// Create directory
	fs, err := d.CreateTemplateDir(record.Path())
	if err != nil {
		return err
	}

	// Create files
	if _, err := d.CreateTemplateManifest(); err != nil {
		return err
	}
	if _, err := d.CreateTemplateInputs(); err != nil {
		return err
	}
	if err := createReadme(o, d, fs); err != nil {
		return err
	}
	return nil
}

func createReadme(o Options, d dependencies, fs filesystem.Fs) error {
	content := "### %s\n\n%s\n\n"
	file := filesystem.NewFile(`README.md`, fmt.Sprintf(content, o.Name, o.Description)).SetDescription(`readme`)
	if err := fs.WriteFile(file); err != nil {
		return err
	}
	d.Logger().Infof("Created readme file \"%s\".", file.Path)
	return nil
}
