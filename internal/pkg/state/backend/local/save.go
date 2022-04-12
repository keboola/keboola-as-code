package local

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/relatedpaths"
)

type saveContext struct {
	*uow
	parentCtx state.SaveContext
	mapperCtx *SaveContext
	backups   map[string]string // soft delete - files rename
}

func (c *saveContext) save() {
	c.
		workersFor(c.parentCtx.Object.Level()).
		AddWorker(func() error {
			if err := c.writeToFs(); err != nil {
				return err
			}
			if err := c.addToManifest(); err != nil {
				return err
			}
			c.parentCtx.OnSuccess()
			return nil
		})
}

func (c *saveContext) writeToFs() error {
	errs := errors.NewMultiError()

	// Invoke mapper
	var err error
	c.mapperCtx, err = c.mapper.MapBeforeLocalSave(c.ctx, c.parentCtx.Object, c.parentCtx.ChangedFields)
	if err != nil {
		return err
	}

	// Existing ctx are backed up, if the operation fails, they will be restored
	c.backups = make(map[string]string)
	defer c.restoreBackups(errs)

	// Load ctx
	toDelete := c.mapperCtx.ToDelete()
	for _, file := range c.mapperCtx.ToSave().All() {
		// Previous versions must be deleted
		toDelete = append(toDelete, file.Path())
	}

	// Delete
	for _, path := range toDelete {
		if err := c.softDelete(path); err != nil {
			errs.Append(err)
		}
	}

	// Stop on error - restore backups
	if errs.Len() > 0 {
		return errs
	}

	// Write new ctx
	relatedPaths := relatedpaths.New(c.mapperCtx.BasePath())
	for _, file := range c.mapperCtx.ToSave().All() {
		// Convert to File, eg. JsonFile -> File
		fileRaw, err := file.ToRawFile()
		if err != nil {
			errs.Append(err)
			continue
		}

		// Write
		relatedPaths.Add(fileRaw.Path())
		if err := c.objectsRoot.WriteFile(fileRaw); err != nil {
			errs.Append(err)
		}
	}

	// Stop on error - restore backups
	if errs.Len() > 0 {
		return errs
	}

	// Cleanup - remove backups
	c.removeBackups()

	// Update related paths
	c.SetRelatedPaths(c.parentCtx.Object, relatedPaths)
	return errs.ErrorOrNil()
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
func (c *saveContext) restoreBackups(errs *errors.MultiError) {
	if errs.Len() > 0 {
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

func (c *saveContext) addToManifest() error {
	// Create manifest
	objectManifest := c.parentCtx.Object.(model.ObjectManifestFactory).NewObjectManifest()

	// Set path
	objectManifest.SetPath(c.mapperCtx.BasePath())

	// Set relations if they are supported
	o, ok1 := c.parentCtx.Object.(model.ObjectWithRelations)
	m, ok2 := objectManifest.(model.ObjectManifestWithRelations)
	if ok1 && ok2 {
		m.SetRelations(o.GetRelations().OnlyStoredInManifest())
	}

	// Add record to manifest
	return c.manifest.Add(objectManifest)
}
