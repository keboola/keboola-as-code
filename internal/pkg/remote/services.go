package remote

import (
	"fmt"

	"github.com/go-resty/resty/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/client"
)

type (
	ServiceId  string
	ServiceUrl string
)

func (a *StorageApi) Services() ([]interface{}, error) {
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

func (a *StorageApi) ServicesUrlById() (map[ServiceId]ServiceUrl, error) {
	services, err := a.Services()
	if err != nil {
		return nil, err
	}

	urls := make(map[ServiceId]ServiceUrl)
	for _, object := range services {
		service := object.(map[string]interface{})
		urls[ServiceId(service["id"].(string))] = ServiceUrl(service["url"].(string))
	}

	return urls, nil
}

func (a *StorageApi) GetServicesRequest() *client.Request {
	result := make(map[string]interface{})
	return a.NewRequest(resty.MethodGet, "/").
		SetQueryParam("exclude", "components").
		SetResult(&result)
}
