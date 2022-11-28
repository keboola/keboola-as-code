package schema

import (
	"strconv"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type exports = PrefixT[model.Export]

type Exports struct {
	exports
}

type ExportsInProject struct {
	exports
}

type ExportsInReceiver struct {
	exports
}

func (v ConfigsRoot) Exports() Exports {
	return Exports{exports: NewTypedPrefix[model.Export](
		v.prefix.Add("export"),
		v.schema.serialization,
	)}
}

func (v Exports) InProject(projectID int) ExportsInProject {
	if projectID == 0 {
		panic(errors.New("export projectID cannot be empty"))
	}
	return ExportsInProject{exports: v.exports.Add(strconv.Itoa(projectID))}
}

func (v ExportsInProject) InReceiver(receiverID string) ExportsInReceiver {
	if receiverID == "" {
		panic(errors.New("export receiverID cannot be empty"))
	}
	return ExportsInReceiver{exports: v.exports.Add(receiverID)}
}

func (v ExportsInReceiver) ID(exportID string) KeyT[model.Export] {
	if exportID == "" {
		panic(errors.New("export exportID cannot be empty"))
	}
	return v.exports.Key(exportID)
}
