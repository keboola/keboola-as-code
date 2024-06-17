package branch

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch/schema"
)

func (r *Repository) List(parentKey keboola.ProjectID) iterator.DefinitionT[definition.Branch] {
	return r.list(r.schema.Active(), parentKey)
}

func (r *Repository) ListDeleted(parentKey keboola.ProjectID) iterator.DefinitionT[definition.Branch] {
	return r.list(r.schema.Deleted(), parentKey)
}

func (r *Repository) list(pfx schema.BranchInState, parentKey keboola.ProjectID) iterator.DefinitionT[definition.Branch] {
	return pfx.InProject(parentKey).GetAll(r.client)
}
