package remote

import (
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type deleteContext struct {
	*uow
	parentCtx state.DeleteContext
}

func (c *deleteContext) delete() {
	// Branch must be deleted in blocking operation
	if branchKey, ok := c.parentCtx.Key.(model.BranchKey); ok {
		if _, err := c.storageApi.DeleteBranch(branchKey); err != nil {
			c.errs.Append(err)
		}

		// Notify UnitOfWork
		c.parentCtx.OnSuccess()
		return
	}

	// Other types can be deleted in parallel
	c.
		poolFor(c.parentCtx.Key.Level()).
		Request(c.storageApi.DeleteRequest(c.parentCtx.Key)).
		OnSuccess(func(response *client.Response) {
			// Notify UnitOfWork
			c.parentCtx.OnSuccess()
		}).
		Send()
}
