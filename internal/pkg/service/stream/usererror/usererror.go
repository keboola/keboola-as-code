// Package usererror provides functions to detect user error (expected error) and application error (unexpected error).
package usererror

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// CheckAndWrap checks the error with Is function.
// If the error is a user error, then the error is wrapped using task.WrapUserError function.
func CheckAndWrap(errPtr *error) {
	if errPtr == nil || *errPtr == nil {
		return
	}

	if err := *errPtr; Is(err) {
		*errPtr = task.WrapUserError(err)
	}
}

// Is returns true if the error is a user error, not an app error.
// For example "storage.invalidToken" error need a user action and cannot be handled by the app code.
func Is(err error) bool {
	var storageAPIErr *keboola.StorageError
	if errors.As(err, &storageAPIErr) && storageAPIErr.ErrCode == "storage.tokenInvalid" {
		return true
	}

	return false
}
