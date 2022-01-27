package storageapi

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (a *Api) CreateRequest(object interface{}) (*client.Request, error) {
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

func (a *Api) UpdateRequest(object interface{}, changed model.ChangedFields) (*client.Request, error) {
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

func (a *Api) DeleteRequest(key model.Key) *client.Request {
	switch k := key.(type) {
	case model.BranchKey:
		return a.DeleteBranchRequest(k)
	case model.ConfigKey:
		return a.DeleteConfigRequest(k)
	case model.ConfigRowKey:
		return a.DeleteConfigRowRequest(k)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, key))
	}
}
