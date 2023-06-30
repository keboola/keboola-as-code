package service

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func checkAndWrapUserError(errPtr *error) {
	if errPtr == nil || *errPtr == nil {
		return
	}

	if err := *errPtr; isUserError(err) {
		*errPtr = task.WrapUserError(err)
	}
}

// isUserError returns true if the error is user, not app error.
// For example "invalid token" error need a user action and cannot be handled by the app.
func isUserError(err error) bool {
	var storageAPIErr *keboola.StorageError
	if errors.As(err, &storageAPIErr) && storageAPIErr.ErrCode == "storage.tokenInvalid" {
		return true
	}

	return false
}
