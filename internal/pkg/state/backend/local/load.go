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

	// Call mappers
	errors := utils.NewMultiError()
	recipe := model.NewLocalLoadRecipe(c.fileLoader(), objectManifest, object)
	if err := c.mapper.MapAfterLocalLoad(recipe); err != nil {
		errors.Append(err)
	}

	// Collect related paths
	relatedPaths := relatedpaths.New(objectManifest.Path())
	for _, file := range recipe.Files.Loaded() {
		relatedPaths.Add(file.Path())
	}
	c.SetRelatedPaths(object.Key(), relatedPaths)

	// Validate, only if all files has been loaded without error, it prevents duplicate errors
	if errors.Len() == 0 {
		if err := validator.Validate(c.ctx, object); err != nil {
			errors.AppendWithPrefix(fmt.Sprintf(`%s is invalid`, objectManifest.String()), err)
		}
	}

	// Is object valid?
	if errors.Len() > 0 {
		return c.invalidObjectError(object, errors)
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
