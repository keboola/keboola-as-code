package schema

import (
	"fmt"
	"strconv"

	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type mappings = PrefixT[model.Mapping]

type Mappings struct {
	mappings
}

type MappingsInProject struct {
	mappings
}

type MappingsInReceiver struct {
	mappings
}

type MappingsInExport struct {
	mappings
}

func (v ConfigsRoot) Mappings() Mappings {
	return Mappings{mappings: NewTypedPrefix[model.Mapping](
		v.prefix.Add("mapping/revision"),
		v.schema.serialization,
	)}
}

func (v Mappings) InExport(k storeKey.ExportKey) MappingsInExport {
	if k.ProjectID == 0 {
		panic(errors.New("mapping projectID cannot be empty"))
	}
	if k.ReceiverID == "" {
		panic(errors.New("mapping receiverID cannot be empty"))
	}
	if k.ExportID == "" {
		panic(errors.New("mapping exportID cannot be empty"))
	}
	return MappingsInExport{mappings: v.mappings.Add(strconv.Itoa(k.ProjectID)).Add(k.ReceiverID).Add(k.ExportID)}
}

func (v Mappings) ByKey(k storeKey.MappingKey) KeyT[model.Mapping] {
	if k.RevisionID == 0 {
		panic(errors.New("mapping revision cannot be empty"))
	}
	return v.InExport(k.ExportKey).Key(fmt.Sprintf("%08d", k.RevisionID))
}
