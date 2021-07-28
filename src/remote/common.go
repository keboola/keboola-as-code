package remote

import (
	"fmt"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
)

func (a *StorageApi) CreateRequest(object interface{}) (*client.Request, error) {
	switch v := object.(type) {
	case *model.Branch:
		return a.CreateBranchRequest(v), nil
	case *model.Config:
		return a.CreateConfigRequest(&model.ConfigWithRows{Config: v})
	case *model.ConfigWithRows:
		return a.CreateConfigRequest(v)
	case *model.ConfigRow:
		return a.CreateConfigRowRequest(v)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

func (a *StorageApi) UpdateRequest(object interface{}, changed []string) (*client.Request, error) {
	switch v := object.(type) {
	case *model.Branch:
		return a.UpdateBranchRequest(v, changed), nil
	case *model.ConfigWithRows:
		return a.UpdateConfigRequest(v.Config, changed)
	case *model.Config:
		return a.UpdateConfigRequest(v, changed)
	case *model.ConfigRow:
		return a.UpdateConfigRowRequest(v, changed)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}
