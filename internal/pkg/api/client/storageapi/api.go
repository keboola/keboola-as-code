package storageapi

import (
	"fmt"

	. "github.com/keboola/keboola-as-code/internal/pkg/api/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func newRequest[R Result](resultDef R) Request[R] {
	// Create request and set default error type
	return NewRequest(resultDef).SetErrorDef(&Error{})
}

func getChangedValues(all map[string]string, changedFields model.ChangedFields) map[string]string {
	// Filter
	data := map[string]string{}
	for key := range changedFields {
		if v, ok := all[key]; ok {
			data[key] = v
		} else {
			panic(fmt.Errorf(`changed field "%s" not found in API values`, key))
		}
	}
	return data
}
