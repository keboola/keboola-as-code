package branch

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository/branch/schema"
)

func (r *Repository) List(parentKey keboola.ProjectID, opts ...iterator.Option) iterator.DefinitionT[definition.Branch] {
	return r.list(r.schema.Active(), parentKey, opts...)
}

func (r *Repository) ListDeleted(parentKey keboola.ProjectID, opts ...iterator.Option) iterator.DefinitionT[definition.Branch] {
	return r.list(r.schema.Deleted(), parentKey, opts...)
}

func (r *Repository) list(pfx schema.BranchInState, parentKey keboola.ProjectID, opts ...iterator.Option) iterator.DefinitionT[definition.Branch] {
	return pfx.InProject(parentKey).GetAll(r.client, opts...)
}
