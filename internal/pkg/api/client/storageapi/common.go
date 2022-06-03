package storageapi

import (
	"fmt"

	. "github.com/keboola/keboola-as-code/internal/pkg/api/client"
	"github.com/keboola/keboola-as-code/internal/pkg/http"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Metadata struct {
	Id        string `json:"id"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp string `json:"timestamp"`
}

func CreateRequest(object any) (Request[, error) {
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

func (a *Api) UpdateRequest(object interface{}, changed model.ChangedFields) (*http.Request, error) {
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

func (a *Api) DeleteRequest(key model.Key) *http.Request {
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

func (a *Api) AppendMetadataRequest(object interface{}) *http.Request {
	switch v := object.(type) {
	case *model.Branch:
		return a.AppendBranchMetadataRequest(v)
	case *model.ConfigWithRows:
		return a.AppendConfigMetadataRequest(v.Config)
	case *model.Config:
		return a.AppendConfigMetadataRequest(v)
	case *model.ConfigRow:
		return nil
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}
