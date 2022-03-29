package local

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/relatedpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const IgnoreNotFoundError = ctxKey("IgnoreNotFoundError")

type ctxKey string

type loadContext struct {
	*uow
	state.LoadContext
	ignoreNotFoundError bool
}

func (c *loadContext) loadAll() {
	c.ignoreNotFoundError, _ = c.ctx.Value(IgnoreNotFoundError).(bool)

	for _, objectManifest := range c.manifest.All() {
		objectManifest := objectManifest
		c.
			workersFor(objectManifest.Level()).
			AddWorker(func() error {
				return c.loadObject(objectManifest)
			})
	}
}

func (c *loadContext) loadObject(objectManifest model.ObjectManifest) error {
	// Get parent key
	parentKey, err := objectManifest.ParentKey()
	if err != nil {
		return err
	}

	// Check if parent is loaded
	if parentKey != nil {
		if _, found := c.Get(parentKey); found {
			// ok
		} else if _, found := c.notFoundObjects[parentKey]; found {
			// Parent and child are missing
			return c.notFoundError(objectManifest)
		} else {
			// Parent and child are ignored by the filter
			return nil
		}
	}

	// Check if directory exists
	if !c.objectsRoot.IsDir(objectManifest.Path().String()) {
		return c.notFoundError(objectManifest)
	}

	// Create empty object
	object := objectManifest.NewEmptyObject()

	// Set relations if they are supported
	o, ok1 := object.(model.ObjectWithRelations)
	m, ok2 := objectManifest.(model.ObjectManifestWithRelations)
	if ok1 && ok2 {
		o.SetRelations(m.GetRelations())
	}

	// Call mappers
	recipe := model.NewLocalLoadRecipe(c.fileLoader(), objectManifest.Path(), object)
	if err := c.mapper.MapAfterLocalLoad(recipe); err != nil {
		return c.invalidObjectError(object, err)
	}

	// Collect related paths
	relatedPaths := relatedpaths.New(objectManifest.Path())
	for _, file := range recipe.Files.Loaded() {
		relatedPaths.Add(file.Path())
	}
	c.SetRelatedPaths(object.Key(), relatedPaths)

	// Validate, only if all files has been loaded without error, it prevents duplicate errors
	if err := validator.Validate(c.ctx, object); err != nil {
		err = utils.PrefixError(fmt.Sprintf(`%s is invalid`, objectManifest.String()), err)
		return c.invalidObjectError(object, err)
	}

	// Work done, notify UnitOfWork
	c.OnLoad(object)
	return nil
}

func (c *loadContext) notFoundError(objectManifest model.ObjectManifest) error {
	c.addNotFoundObject(objectManifest)
	if c.ignoreNotFoundError {
		return nil
	}
	return fmt.Errorf(`%s not found`, objectManifest.String())
}

func (c *loadContext) invalidObjectError(object model.Object, err error) error {
	c.addInvalidObject(object)
	return err
}
