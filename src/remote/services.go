package remote

import (
	"fmt"

	"github.com/go-resty/resty/v2"

	"keboola-as-code/src/client"
)

func (a *StorageApi) GetServices() ([]interface{}, error) {
	request := a.GetServicesRequest()
	response := request.Send().Response
	if response.HasResult() {
		storageIndex := *response.Result().(*map[string]interface{})
		if services, ok := storageIndex["services"]; ok {
			return services.([]interface{}), nil
		} else {
			return nil, fmt.Errorf("services array not found in Storage API index info: %v", storageIndex)
		}
	}
	return nil, response.Err()
}

func (a *StorageApi) GetEncryptionApiUrl() (string, error) {
	services, err := a.GetServices()
	if err != nil {
		return "", err
	}

	for _, object := range services {
		service := object.(map[string]interface{})
		if service["id"] == "encryption" {
			url := service["url"]
			return url.(string), nil
		}
	}
	return "", fmt.Errorf("encryption API not found in services from Storage API: \"%s\"", services)
}

func (a *StorageApi) GetServicesRequest() *client.Request {
	result := make(map[string]interface{})
	return a.NewRequest(resty.MethodGet, "/").
		SetQueryParam("exclude", "components").
		SetResult(&result)
}
