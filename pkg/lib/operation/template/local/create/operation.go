package create

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacekeys"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	saveRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/save"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/template/sync/pull"
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
	Branch      model.BranchKey
	Configs     []ConfigDef
}

func (o *Options) ObjectsFilter() model.ObjectsFilter {
	var keys []model.Key

	// Branch
	keys = append(keys, o.Branch)

	// Configs and rows
	for _, config := range o.Configs {
		keys = append(keys, config.Key)
		for _, row := range config.Rows {
			keys = append(keys, row.Key)
		}
	}

	filter := model.NoFilter()
	filter.SetAllowedKeys(keys)
	return filter
}

func (o *Options) Replacements() replacekeys.Keys {
	var keys replacekeys.Keys

	// Branch
	keys = append(keys, replacekeys.Key{Old: o.Branch, New: model.BranchKey{Id: 0}})

	// Configs and rows
	for _, config := range o.Configs {
		newConfigId := model.ConfigId(jsonnet.ConfigIdPlaceholder(config.TemplateId))
		newConfigKey := config.Key
		newConfigKey.BranchId = 0
		newConfigKey.Id = newConfigId
		keys = append(keys, replacekeys.Key{Old: config.Key, New: newConfigKey})
		for _, row := range config.Rows {
			newRowId := model.RowId(jsonnet.ConfigRowIdPlaceholder(row.TemplateId))
			newRowKey := row.Key
			newRowKey.BranchId = 0
			newRowKey.ConfigId = newConfigId
			newRowKey.Id = newRowId
			keys = append(keys, replacekeys.Key{Old: row.Key, New: newRowKey})
		}
	}
	return keys
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	TemplateRepositoryDir() (filesystem.Fs, error)
	TemplateRepositoryManifest() (*repositoryManifest.Manifest, error)
	TemplateSrcDir() (filesystem.Fs, error)
	TemplateManifest() (*template.Manifest, error)
	TemplateState(loadOptions loadState.OptionsWithFilter, replacements replacekeys.Keys) (*template.State, error)
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

	// Pull remote objects
	pullOptions := pull.Options{
		RemoteFilter: o.ObjectsFilter(),
		Replacements: o.Replacements(),
	}
	if err := pull.Run(pullOptions, d); err != nil {
		return err
	}

	// Done
	d.Logger().Infof(`Template "%s" has been created.`, versionRecord.Path())

	return nil
}

func initTemplateDir(o Options, d dependencies, record repositoryManifest.VersionRecord) (err error) {
	// Create directory
	fs, err := d.CreateTemplateDir(record.Path())
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
	file := filesystem.NewRawFile(`README.md`, fmt.Sprintf(content, o.Name, o.Description)).SetDescription(`readme`)
	if err := fs.WriteFile(file); err != nil {
		return err
	}
	d.Logger().Infof("Created readme file \"%s\".", file.Path())
	return nil
}
