package remote

import (
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"

	"github.com/go-resty/resty/v2"
	"github.com/spf13/cast"
)

func (a *StorageApi) ListComponents(branchId int) (*[]*model.ComponentWithConfigs, error) {
	response := a.ListComponentsRequest(branchId).Send().Response
	if response.HasResult() {
		return response.Result().(*[]*model.ComponentWithConfigs), nil
	}
	return nil, response.Err()
}

func (a *StorageApi) GetConfig(branchId int, componentId string, configId string) (*model.Config, error) {
	response := a.GetConfigRequest(branchId, componentId, configId).Send().Response
	if response.HasResult() {
		return response.Result().(*model.Config), nil
	}
	return nil, response.Err()
}

func (a *StorageApi) CreateConfig(config *model.ConfigWithRows) (*model.ConfigWithRows, error) {
	request, err := a.CreateConfigRequest(config)
	if err != nil {
		return nil, err
	}

	response := request.Send().Response
	if response.HasResult() {
		return response.Result().(*model.ConfigWithRows), nil
	}
	return nil, response.Err()
}

func (a *StorageApi) UpdateConfig(config *model.Config, changed []string) (*model.Config, error) {
	request, err := a.UpdateConfigRequest(config, changed)
	if err != nil {
		return nil, err
	}

	response := request.Send().Response
	if response.HasResult() {
		return response.Result().(*model.Config), nil
	}
	return nil, response.Err()
}

// DeleteConfig - only config in main branch can be deleted!
func (a *StorageApi) DeleteConfig(componentId string, configId string) error {
	return a.DeleteConfigRequest(componentId, configId).Send().Err()
}

func (a *StorageApi) ListComponentsRequest(branchId int) *client.Request {
	components := make([]*model.ComponentWithConfigs, 0)
	return a.
		NewRequest(resty.MethodGet, "branch/{branchId}/components").
		SetPathParam("branchId", cast.ToString(branchId)).
		SetQueryParam("include", "configuration,rows").
		SetResult(&components).
		OnSuccess(func(response *client.Response) {
			if response.Result() != nil {
				// Add missing values
				for _, component := range components {
					component.BranchId = branchId

					// Cache
					a.Components().Set(component.Component)

					// Set config IDs
					for _, config := range component.Configs {
						config.BranchId = branchId
						config.ComponentId = component.Id
						config.SortRows()

						// Set rows IDs
						for _, row := range config.Rows {
							row.BranchId = branchId
							row.ComponentId = component.Id
							row.ConfigId = config.Id
						}
					}
				}
			}
		})
}

// GetConfigRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/manage-configurations/development-branch-configuration-detail
func (a *StorageApi) GetConfigRequest(branchId int, componentId string, configId string) *client.Request {
	config := &model.Config{}
	config.BranchId = branchId
	config.ComponentId = componentId
	return a.
		NewRequest(resty.MethodGet, "branch/{branchId}/components/{componentId}/configs/{configId}").
		SetPathParam("branchId", cast.ToString(branchId)).
		SetPathParam("componentId", componentId).
		SetPathParam("configId", configId).
		SetResult(config)
}

// CreateConfigRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/create-development-branch-configuration
func (a *StorageApi) CreateConfigRequest(config *model.ConfigWithRows) (*client.Request, error) {
	// Data
	values, err := config.ToApiValues()
	if err != nil {
		return nil, err
	}

	// Create config with the defined ID
	if config.Id != "" {
		values["configurationId"] = config.Id
	}

	// Create config
	var configRequest *client.Request
	configRequest = a.
		NewRequest(resty.MethodPost, "branch/{branchId}/components/{componentId}/configs").
		SetPathParam("branchId", cast.ToString(config.BranchId)).
		SetPathParam("componentId", config.ComponentId).
		SetBody(values).
		SetResult(config).
		// Create config rows
		OnSuccess(func(response *client.Response) {
			for _, row := range config.Rows {
				row.BranchId = config.BranchId
				row.ComponentId = config.ComponentId
				row.ConfigId = config.Id
				rowRequest, err := a.CreateConfigRowRequest(row)
				if err != nil {
					response.SetErr(err)
				}
				configRequest.WaitFor(rowRequest)
				response.Sender().Request(rowRequest).Send()
			}
		})

	return configRequest, nil
}

// UpdateConfigRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/manage-configurations/update-development-branch-configuration
func (a *StorageApi) UpdateConfigRequest(config *model.Config, changed []string) (*client.Request, error) {
	// Id is required
	if config.Id == "" {
		panic("config id must be set")
	}

	// Data
	values, err := config.ToApiValues()
	if err != nil {
		return nil, err
	}

	// Update config
	request := a.
		NewRequest(resty.MethodPut, "branch/{branchId}/components/{componentId}/configs/{configId}").
		SetPathParam("branchId", cast.ToString(config.BranchId)).
		SetPathParam("componentId", config.ComponentId).
		SetPathParam("configId", config.Id).
		SetBody(getChangedValues(values, changed)).
		SetResult(config)

	return request, nil
}

// DeleteConfigRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/manage-configurations/delete-configuration
// Only config in main branch can be deleted!
func (a *StorageApi) DeleteConfigRequest(componentId string, configId string) *client.Request {
	return a.NewRequest(resty.MethodDelete, "components/{componentId}/configs/{configId}").
		SetPathParam("componentId", componentId).
		SetPathParam("configId", configId)
}

func (a *StorageApi) DeleteConfigsInBranchRequest(branchId int) *client.Request {
	return a.ListComponentsRequest(branchId).
		OnSuccess(func(response *client.Response) {
			for _, component := range *response.Result().(*[]*model.ComponentWithConfigs) {
				for _, config := range component.Configs {
					response.
						Sender().
						Request(a.DeleteConfigRequest(config.ComponentId, config.Id)).
						Send()
				}
			}
		})
}
