package storageapi

import . "github.com/keboola/keboola-as-code/internal/pkg/api/client"

func newRequest[R Result](resultDef R) Request[R] {
	// Create request and set default error type
	return NewRequest(resultDef).SetErrorDef(&Error{})
}
