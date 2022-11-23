package schema

import (
	"fmt"
	"strconv"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type Mappings struct {
	prefix
}

type MappingsInProject struct {
	prefix
}

type MappingsInReceiver struct {
	prefix
}

type MappingsInExport struct {
	prefix
}

func (v ConfigsRoot) Mappings() Mappings {
	return Mappings{prefix: v.prefix.Add("mapping/revision")}
}

func (v Mappings) InProject(projectID int) MappingsInProject {
	return MappingsInProject{prefix: v.prefix.Add(strconv.Itoa(projectID))}
}

func (v MappingsInProject) InReceiver(receiverID string) MappingsInReceiver {
	return MappingsInReceiver{prefix: v.prefix.Add(receiverID)}
}

func (v MappingsInReceiver) InExport(exportID string) MappingsInExport {
	return MappingsInExport{prefix: v.prefix.Add(exportID)}
}

func (v MappingsInExport) Revision(revision int) Key {
	return v.prefix.Key(fmt.Sprintf("%08d", revision))
}
