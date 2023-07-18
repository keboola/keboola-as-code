package service

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// checkAndWrapUserError checks the error with isUserError function.
// If the error is a user error, then the error is wrapped using task.WrapUserError function.
func checkAndWrapUserError(errPtr *error) {
	if errPtr == nil || *errPtr == nil {
		return
	}

	if err := *errPtr; isUserError(err) {
		*errPtr = task.WrapUserError(err)
	}
}

// isUserError returns true if the error is a user error, not an app error.
// For example "storage.invalidToken" error need a user action and cannot be handled by the app code.
func isUserError(err error) bool {
	var storageAPIErr *keboola.StorageError
	if errors.As(err, &storageAPIErr) && storageAPIErr.ErrCode == "storage.tokenInvalid" {
		return true
	}

	return false
}
