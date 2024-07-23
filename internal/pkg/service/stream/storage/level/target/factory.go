package target

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	targetModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/model"
)

func NewTarget(fileImportCfg config.ImportConfig) targetModel.Target {
	return targetModel.Target{
		Import: fileImportCfg,
	}
}
