package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type deleteContext struct {
	*uow
	state.DeleteContext
}

func (c *deleteContext) delete() {
	c.
		workersFor(c.Key.Level()).
		AddWorker(func() error {
			// Remove manifest record
			c.manifest.Remove(c.Key)

			// Remove all related files
			errs := errors.NewMultiError()
			if relatedPaths, found := c.GetRelatedPathsByKey(c.Key); found {
				for _, path := range relatedPaths.All() {
					if c.objectsRoot.IsFile(path) {
						if err := c.objectsRoot.Remove(path); err != nil {
							errs.Append(err)
						}
					}
				}
			}

			if errors.Len() == 0 {
				// Remove the key from the auxiliary maps
				delete(c.notFoundObjects, c.Key)
				delete(c.invalidObjects, c.Key)

				// Notify UnitOfWork
				c.OnSuccess()
			}

			return errs.ErrorOrNil()
		})
}
