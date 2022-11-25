package schema

import (
	"fmt"
	"strconv"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
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

func (v Mappings) InProject(projectID int) MappingsInProject {
	if projectID == 0 {
		panic(errors.New("mapping projectID cannot be empty"))
	}
	return MappingsInProject{mappings: v.mappings.Add(strconv.Itoa(projectID))}
}

func (v MappingsInProject) InReceiver(receiverID string) MappingsInReceiver {
	if receiverID == "" {
		panic(errors.New("mapping receiverID cannot be empty"))
	}
	return MappingsInReceiver{mappings: v.mappings.Add(receiverID)}
}

func (v MappingsInReceiver) InExport(exportID string) MappingsInExport {
	if exportID == "" {
		panic(errors.New("mapping exportID cannot be empty"))
	}
	return MappingsInExport{mappings: v.mappings.Add(exportID)}
}

func (v MappingsInExport) Revision(revision int) KeyT[model.Mapping] {
	if revision == 0 {
		panic(errors.New("mapping revision cannot be empty"))
	}
	return v.mappings.Key(fmt.Sprintf("%08d", revision))
}
