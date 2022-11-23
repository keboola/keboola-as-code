package schema

import (
	"fmt"
	"strconv"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	if projectID == 0 {
		panic(errors.New("mapping projectID cannot be empty"))
	}
	return MappingsInProject{prefix: v.prefix.Add(strconv.Itoa(projectID))}
}

func (v MappingsInProject) InReceiver(receiverID string) MappingsInReceiver {
	if receiverID == "" {
		panic(errors.New("mapping receiverID cannot be empty"))
	}
	return MappingsInReceiver{prefix: v.prefix.Add(receiverID)}
}

func (v MappingsInReceiver) InExport(exportID string) MappingsInExport {
	if exportID == "" {
		panic(errors.New("mapping exportID cannot be empty"))
	}
	return MappingsInExport{prefix: v.prefix.Add(exportID)}
}

func (v MappingsInExport) Revision(revision int) Key {
	if revision == 0 {
		panic(errors.New("mapping revision cannot be empty"))
	}
	return v.prefix.Key(fmt.Sprintf("%08d", revision))
}
