package repository

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

type (
	branchSchema        struct{ PrefixT[definition.Branch] }
	branchSchemaInState branchSchema
)

func newBranchSchema(s *serde.Serde) branchSchema {
	return branchSchema{PrefixT: NewTypedPrefix[definition.Branch]("definition/branch", s)}
}

// Active prefix contains all not deleted objects.
func (v branchSchema) Active() branchSchemaInState {
	return branchSchemaInState{PrefixT: v.PrefixT.Add("active")}
}

// Deleted prefix contains all deleted objects whose parent existed on deleted.
func (v branchSchema) Deleted() branchSchemaInState {
	return branchSchemaInState{PrefixT: v.PrefixT.Add("deleted")}
}

func (v branchSchemaInState) InProject(k keboola.ProjectID) PrefixT[definition.Branch] {
	return v.PrefixT.Add(k.String())
}

func (v branchSchemaInState) ByKey(k BranchKey) KeyT[definition.Branch] {
	return v.PrefixT.Key(k.String())
}
