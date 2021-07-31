package remote

import (
	"fmt"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"

	"github.com/go-resty/resty/v2"
	"github.com/spf13/cast"
)

func (a *StorageApi) GetConfigRow(branchId int, componentId string, configId string, rowId string) (*model.ConfigRow, error) {
	response := a.GetConfigRowRequest(branchId, componentId, configId, rowId).Send().Response
	if response.HasResult() {
		return response.Result().(*model.ConfigRow), nil
	}
	return nil, response.Err()
}

func (a *StorageApi) CreateConfigRow(row *model.ConfigRow) (*model.ConfigRow, error) {
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

func (a *StorageApi) UpdateConfigRow(row *model.ConfigRow, changed []string) (*model.ConfigRow, error) {
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

// DeleteConfigRow - only config row in main branch can be deleted!
func (a *StorageApi) DeleteConfigRow(componentId string, configId string, rowId string) error {
	return a.DeleteConfigRowRequest(componentId, configId, rowId).Send().Err()
}

// GetConfigRowRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/manage-configuration-rows/row-detail
func (a *StorageApi) GetConfigRowRequest(branchId int, componentId string, configId string, rowId string) *client.Request {
	row := &model.ConfigRow{}
	row.BranchId = branchId
	row.ComponentId = componentId
	row.ConfigId = configId
	return a.
		NewRequest(resty.MethodGet, fmt.Sprintf("branch/%d/components/%s/configs/%s/rows/%s", branchId, componentId, configId, rowId)).
		SetResult(row)
}

// CreateConfigRowRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/create-or-list-configuration-rows/create-development-branch-configuration-row
func (a *StorageApi) CreateConfigRowRequest(row *model.ConfigRow) (*client.Request, error) {
	// Data
	values, err := row.ToApiValues()
	if err != nil {
		return nil, err
	}

	// Create row with the defined ID
	if row.Id != "" {
		values["rowId"] = row.Id
	}

	// Create request
	request := a.
		NewRequest(resty.MethodPost, "branch/{branchId}/components/{componentId}/configs/{configId}/rows").
		SetPathParam("branchId", cast.ToString(row.BranchId)).
		SetPathParam("componentId", row.ComponentId).
		SetPathParam("configId", row.ConfigId).
		SetBody(values).
		SetResult(row)

	return request, nil
}

// UpdateConfigRowRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/manage-configuration-rows/update-row-for-development-branch
func (a *StorageApi) UpdateConfigRowRequest(row *model.ConfigRow, changed []string) (*client.Request, error) {
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
		NewRequest(resty.MethodPut, "branch/{branchId}/components/{componentId}/configs/{configId}/rows/{rowId}").
		SetPathParam("branchId", cast.ToString(row.BranchId)).
		SetPathParam("componentId", row.ComponentId).
		SetPathParam("configId", row.ConfigId).
		SetPathParam("rowId", row.Id).
		SetBody(getChangedValues(values, changed)).
		SetResult(row)

	return request, nil
}

// DeleteConfigRowRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/manage-configuration-rows/update-row
// Only config in main branch can be removed!
func (a *StorageApi) DeleteConfigRowRequest(componentId string, configId string, rowId string) *client.Request {
	return a.NewRequest(resty.MethodDelete, "components/{componentId}/configs/{configId}/rows/{rowId}").
		SetPathParam("componentId", componentId).
		SetPathParam("configId", configId).
		SetPathParam("rowId", rowId)
}
