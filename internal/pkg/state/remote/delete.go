package remote

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type deleteCtx struct {
	*uow
	key       model.Key
	onSuccess func()
}

func (c *deleteCtx) delete() {
	// Branch must be deleted in blocking operation
	if branchKey, ok := c.key.(model.BranchKey); ok {
		if _, err := c.storageApi.DeleteBranch(branchKey); err != nil {
			c.errors.Append(err)
		}
		return
	}

	// Other types can be deleted in parallel
	c.
		poolFor(c.key.Level()).
		Request(c.storageApi.DeleteRequest(c.key)).
		Send()
}
