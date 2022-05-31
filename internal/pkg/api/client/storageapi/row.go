package storageapi

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/http"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (a *Api) GetConfigRow(branchId model.BranchId, componentId model.ComponentId, configId model.ConfigId, rowId model.RowId) (*model.ConfigRow, error) {
	response := a.GetConfigRowRequest(branchId, componentId, configId, rowId).Send().Response
	if response.HasResult() {
		return response.Result().(*model.ConfigRow), nil
	}
	return nil, response.Err()
}

func (a *Api) CreateConfigRow(row *model.ConfigRow) (*model.ConfigRow, error) {
	request, err := a.CreateConfigRowRequest(row)
	if err != nil {
		return nil, err
	}

	response := request.Send().Response
	if response.HasResult() {
		return response.Result().(*model.ConfigRow), nil
	}
	return nil, response.Err()
}

func (a *Api) UpdateConfigRow(row *model.ConfigRow, changed model.ChangedFields) (*model.ConfigRow, error) {
	request, err := a.UpdateConfigRowRequest(row, changed)
	if err != nil {
		return nil, err
	}

	response := request.Send().Response
	if response.HasResult() {
		return response.Result().(*model.ConfigRow), nil
	}
	return nil, response.Err()
}

func (a *Api) DeleteConfigRow(row model.ConfigRowKey) error {
	return a.DeleteConfigRowRequest(row).Send().Err()
}

// GetConfigRowRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/manage-configuration-rows/row-detail
func (a *Api) GetConfigRowRequest(branchId model.BranchId, componentId model.ComponentId, configId model.ConfigId, rowId model.RowId) *http.Request {
	row := &model.ConfigRow{}
	row.BranchId = branchId
	row.ComponentId = componentId
	row.ConfigId = configId
	return a.
		NewRequest(http.MethodGet, fmt.Sprintf("branch/%d/components/%s/configs/%s/rows/%s", branchId, componentId, configId, rowId)).
		SetResult(row)
}

// CreateConfigRowRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/create-or-list-configuration-rows/create-development-branch-configuration-row
func (a *Api) CreateConfigRowRequest(row *model.ConfigRow) (*http.Request, error) {
	// Data
	values, err := row.ToApiValues()
	if err != nil {
		return nil, err
	}

	// Create row with the defined ID
	if row.Id != "" {
		values["rowId"] = row.Id.String()
	}

	// Create request
	request := a.
		NewRequest(http.MethodPost, "branch/{branchId}/components/{componentId}/configs/{configId}/rows").
		SetPathParam("branchId", row.BranchId.String()).
		SetPathParam("componentId", row.ComponentId.String()).
		SetPathParam("configId", row.ConfigId.String()).
		SetFormBody(values).
		SetResult(row)

	return request, nil
}

// UpdateConfigRowRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/manage-configuration-rows/update-row-for-development-branch
func (a *Api) UpdateConfigRowRequest(row *model.ConfigRow, changed model.ChangedFields) (*http.Request, error) {
	// Id is required
	if row.Id == "" {
		panic("config row id must be set")
	}

	// Data
	values, err := row.ToApiValues()
	if err != nil {
		return nil, err
	}

	// Create request
	request := a.
		NewRequest(http.MethodPut, "branch/{branchId}/components/{componentId}/configs/{configId}/rows/{rowId}").
		SetPathParam("branchId", row.BranchId.String()).
		SetPathParam("componentId", row.ComponentId.String()).
		SetPathParam("configId", row.ConfigId.String()).
		SetPathParam("rowId", row.Id.String()).
		SetFormBody(getChangedValues(values, changed)).
		SetResult(row)

	return request, nil
}

// DeleteConfigRowRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/manage-configuration-rows/update-row
func (a *Api) DeleteConfigRowRequest(key model.ConfigRowKey) *http.Request {
	return a.NewRequest(http.MethodDelete, "branch/{branchId}/components/{componentId}/configs/{configId}/rows/{rowId}").
		SetPathParam("branchId", key.BranchId.String()).
		SetPathParam("componentId", key.ComponentId.String()).
		SetPathParam("configId", key.ConfigId.String()).
		SetPathParam("rowId", key.Id.String())
}
