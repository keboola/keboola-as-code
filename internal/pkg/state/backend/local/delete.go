package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type deleteContext struct {
	*uow
	parentCtx state.DeleteContext
}

func (c *deleteContext) delete() {
	c.
		workersFor(c.parentCtx.Key.Level()).
		AddWorker(func() error {
			// Remove manifest record
			c.manifest.Remove(c.parentCtx.Key)

			// Remove all related files
			errs := errors.NewMultiError()
			if relatedPaths, found := c.GetRelatedPathsByKey(c.parentCtx.Key); found {
				for _, path := range relatedPaths.All() {
					if c.objectsRoot.IsFile(path) {
						if err := c.objectsRoot.Remove(path); err != nil {
							errs.Append(err)
						}
					}
				}
			}

			if errs.Len() == 0 {
				// Remove the key from the auxiliary maps
				delete(c.notFoundObjects, c.parentCtx.Key)
				delete(c.invalidObjects, c.parentCtx.Key)

				// Notify UnitOfWork
				c.parentCtx.OnSuccess()
			}

			return errs.ErrorOrNil()
		})
}
