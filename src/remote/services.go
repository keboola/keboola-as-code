package remote

import (
	"fmt"
	"keboola-as-code/src/client"

	"github.com/go-resty/resty/v2"
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

func (a *StorageApi) GetServicesRequest() *client.Request {
	result := make(map[string]interface{})
	return a.NewRequest(resty.MethodGet, "/").
		SetQueryParam("exclude", "components").
		SetResult(&result)
}
