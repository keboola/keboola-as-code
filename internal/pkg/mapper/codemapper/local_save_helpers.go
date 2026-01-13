package codemapper

import (
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (w *localWriter) injectDefaultVenv(config *model.Config) {
	// Get parameters from configuration
	parameters, ok := config.Content.Get("parameters")
	if !ok {
		// Create parameters if missing (should not happen for valid check-config but safe to add)
		parameters = orderedmap.New()
		config.Content.Set("parameters", parameters)
	}

	// Convert to OrderedMap
	paramsMap, ok := parameters.(*orderedmap.OrderedMap)
	if !ok {
		return // Should handle error but here we just skip injection if format is unexpected
	}

	// Check if venv exists
	if _, found := paramsMap.Get("venv"); !found {
		paramsMap.Set("venv", "base")
	}
}
