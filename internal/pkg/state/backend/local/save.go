package local

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/relatedpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type saveContext struct {
	*uow
	state.SaveContext
	basePath model.AbsPath
	backups  map[string]string
	errors   *utils.MultiError
}

func (c *saveContext) save() {
	c.
		workersFor(c.Object.Level()).
		AddWorker(func() error {
			if err := c.resolvePath(); err != nil {
				return err
			}

			if err := c.writeToFs(); err != nil {
				return err
			}

			c.addToManifest()
			return nil
		})
}

func (c *saveContext) resolvePath() error {
	if path, err := c.namingGenerator.GetOrGenerate(c.Object); err == nil {
		c.basePath = path
		return nil
	} else {
		return err
	}
}

func (c *saveContext) writeToFs() error {
	c.errors = utils.NewMultiError()

	// Invoke mapper
	recipe := model.NewLocalSaveRecipe(c.basePath, c.Object, c.ChangedFields)
	if err := c.mapper.MapBeforeLocalSave(recipe); err != nil {
		return err
	}

	// Existing files are backed up, if the operation fails, they will be restored
	c.backups = make(map[string]string)
	defer c.restoreBackups()

	// Load files
	toDelete := recipe.ToDelete
	for _, file := range recipe.Files.All() {
		// Previous versions must be deleted
		toDelete = append(toDelete, file.Path())
	}

	// Delete
	for _, path := range toDelete {
		if err := c.softDelete(path); err != nil {
			c.errors.Append(err)
		}
	}

	// Stop on error - restore backups
	if c.errors.Len() > 0 {
		return c.errors
	}

	// Write new files
	relatedPaths := relatedpaths.New(c.basePath)
	for _, file := range recipe.Files.All() {
		// Convert to File, eg. JsonFile -> File
		fileRaw, err := file.ToRawFile()
		if err != nil {
			c.errors.Append(err)
			continue
		}

		// Write
		relatedPaths.Add(fileRaw.Path())
		if err := c.objectsRoot.WriteFile(fileRaw); err != nil {
			c.errors.Append(err)
		}
	}

	// Stop on error - restore backups
	if c.errors.Len() > 0 {
		return c.errors
	}

	// Cleanup - remove backups
	c.removeBackups()

	// Update related paths
	c.SetRelatedPaths(c.Object, relatedPaths)
	return c.errors.ErrorOrNil()
}

func (c *saveContext) softDelete(path string) error {
	src := path
	dst := src + `.old`
	if !c.objectsRoot.IsFile(src) {
		return nil
	}

	err := c.objectsRoot.Move(src, dst)
	if err == nil {
		c.backups[src] = dst
	}
	return err
}

// restoreBackups if operation fails.
func (c *saveContext) restoreBackups() {
	if c.errors.Len() > 0 {
		for dst, src := range c.backups {
			if err := c.objectsRoot.Move(src, dst); err != nil {
				c.logger.Debug(fmt.Errorf(`cannot restore backup "%s" -> "%s": %w`, src, dst, err))
			}
		}
	}
}

// removeBackups if all is ok.
func (c *saveContext) removeBackups() {
	for _, path := range c.backups {
		if err := c.objectsRoot.Remove(path); err != nil {
			c.logger.Debug(fmt.Errorf(`cannot remove backup "%s": %w`, path, err))
		}
	}
}

func (c *saveContext) addToManifest() {
	// Create manifest
	objectManifest := c.Object.(model.ObjectManifestFactory).NewObjectManifest()

	// Set path
	objectManifest.SetPath(c.basePath)

	// Set relations if they are supported
	o, ok1 := c.Object.(model.ObjectWithRelations)
	m, ok2 := objectManifest.(model.ObjectManifestWithRelations)
	if ok1 && ok2 {
		m.SetRelations(o.GetRelations().OnlyStoredInManifest())
	}

	// Add record to manifest
	if err := c.manifest.Add(objectManifest); err != nil {
		c.errors.Append(err)
	}
}
