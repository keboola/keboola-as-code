package rpc

import (
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type sliceData struct {
	SliceKey     model.SliceKey   `json:"sliceKey"`
	LocalStorage localModel.Slice `json:"localStorage"`
}
