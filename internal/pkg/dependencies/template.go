package dependencies

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacekeys"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	createTemplateDir "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/dir/create"
	createTemplateInputs "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/inputs/create"
	loadTemplateInputs "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/inputs/load"
	createTemplateManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/manifest/create"
	loadTemplateManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/manifest/load"
)

var ErrTemplateManifestNotFound = fmt.Errorf("template manifest not found")

func (c *common) Template(replacements replacekeys.Keys) (*template.Template, error) {
	if c.template == nil {
		templateDir, err := c.TemplateDir()
		if err != nil {
			return nil, err
		}
		manifest, err := c.TemplateManifest()
		if err != nil {
			return nil, err
		}
		inputs, err := c.TemplateInputs()
		if err != nil {
			return nil, err
		}
		c.template = template.New(templateDir, manifest, inputs, replacements, c)
	}
	return c.template, nil
}

func (c *common) TemplateState(loadOptions loadState.OptionsWithFilter, replacements replacekeys.Keys) (*template.State, error) {
	if c.templateState == nil {
		// Get template
		tmpl, err := c.Template(replacements)
		if err != nil {
			return nil, err
		}

		// Run operation
		if state, err := loadState.Run(tmpl, loadOptions, c); err == nil {
			c.templateState = template.NewState(state, tmpl)
		} else {
			return nil, err
		}
	}
	return c.templateState, nil
}

func (c *common) TemplateDir() (filesystem.Fs, error) {
	if c.templateDir == nil {
		// Get FS
		fs := c.Fs()

		// Check if manifest exists
		if !c.TemplateManifestExists() {
			return nil, ErrTemplateManifestNotFound
		}

		// Get repository manifest
		repositoryManifest, err := c.TemplateRepositoryManifest()
		if err != nil {
			return nil, err
		}

		// Split path to parts
		parts := strings.SplitN(fs.WorkingDir(), string(filesystem.PathSeparator), 3)
		templatePart := parts[0]
		versionPart := parts[1]
		templateDir := filesystem.Join(templatePart, versionPart)

		// Check if is template defined in the repository manifest.
		if templateRecord, found := repositoryManifest.GetByPath(templatePart); !found {
			return nil, fmt.Errorf(`template dir "%s" not found in the repository manifest`, parts[0])
		} else if _, found := templateRecord.GetByPath(versionPart); !found {
			return nil, fmt.Errorf(`template version dir "%s" not found in the repository manifest`, templateDir)
		}

		// Get FS for the template dir.
		c.templateDir, err = fs.SubDirFs(templateDir)
		if err != nil {
			return nil, err
		}
	}
	return c.templateDir, nil
}

func (c *common) TemplateManifestExists() bool {
	// Is manifest loaded?
	if c.templateManifest != nil {
		return true
	}

	if !c.TemplateRepositoryManifestExists() {
		return false
	}

	// Get FS
	fs := c.Fs()

	// Template dir is [template]/[version], for example "my-template/v1".
	// Working dir must be the template dir or a subdir.
	parts := strings.SplitN(fs.WorkingDir(), string(filesystem.PathSeparator), 3)
	if len(parts) < 2 {
		return false
	}

	templateDir := strings.Join(parts[0:2], string(filesystem.PathSeparator))
	manifestPath := filesystem.Join(templateDir, templateManifest.Path())
	return fs.IsFile(manifestPath)
}

func (c *common) TemplateManifest() (*template.Manifest, error) {
	if c.templateManifest == nil {
		if m, err := loadTemplateManifest.Run(c); err == nil {
			c.templateManifest = m
		} else {
			return nil, err
		}
	}
	return c.templateManifest, nil
}

func (c *common) TemplateInputs() (*template.Inputs, error) {
	if c.templateInputs == nil {
		if inputs, err := loadTemplateInputs.Run(c); err == nil {
			c.templateInputs = inputs
		} else {
			return nil, err
		}
	}
	return c.templateInputs, nil
}

func (c *common) CreateTemplateDir(path string) (filesystem.Fs, error) {
	if fs, err := createTemplateDir.Run(createTemplateDir.Options{Path: path}, c); err == nil {
		c.templateDir = fs
		return fs, nil
	} else {
		return nil, err
	}
}

func (c *common) CreateTemplateInputs() (*template.Inputs, error) {
	if inputs, err := createTemplateInputs.Run(c); err == nil {
		c.templateInputs = inputs
		return inputs, nil
	} else {
		return nil, err
	}
}

func (c *common) CreateTemplateManifest() (*template.Manifest, error) {
	if m, err := createTemplateManifest.Run(c); err == nil {
		c.templateManifest = m
		return m, nil
	} else {
		return nil, fmt.Errorf(`cannot create manifest: %w`, err)
	}
}
