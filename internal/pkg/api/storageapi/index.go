package storageapi

import (
	"sort"
	"strings"

	"github.com/go-resty/resty/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Index struct {
	Components []*model.Component
}

// NewComponentList - return all published and non-deprecated components.
// Excluded are processors and code patterns.
// Components are ordered by ID, first are all from "keboola" vendor.
func (a *Api) NewComponentList() ([]*model.Component, error) {
	// Load all components
	allComponents, err := a.ListAllComponents()
	if err != nil {
		return nil, err
	}

	// Filter out:
	//	- deprecated
	//  - not published
	//  - processors
	//  - code patterns
	components := make([]*model.Component, 0)
	for _, c := range allComponents {
		if !c.IsDeprecated() && !c.IsExcludedFromNewList() && !c.IsCodePattern() && !c.IsProcessor() {
			components = append(components, c)
		}
	}

	// Sort "keboola" vendor first
	sort.SliceStable(components, func(i, j int) bool {
		idI := components[i].Id
		idJ := components[j].Id

		// Components from keboola vendor will be first
		vendor := `keboola.`
		vendorI := strings.HasPrefix(string(idI), vendor)
		vendorJ := strings.HasPrefix(string(idJ), vendor)
		if vendorI != vendorJ {
			return vendorI
		}

		// Sort by ID otherwise
		return idI < idJ
	})

	return components, nil
}

func (a *Api) ListAllComponents() ([]*model.Component, error) {
	response := a.IndexRequest().Send().Response
	if response.HasResult() {
		return response.Result().(*Index).Components, nil
	}
	return nil, response.Err()
}

func (a *Api) IndexRequest() *client.Request {
	index := &Index{}
	return a.
		NewRequest(resty.MethodGet, "").
		SetResult(index)
}
