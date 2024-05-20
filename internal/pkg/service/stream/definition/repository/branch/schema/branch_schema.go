package schema

import (
	"github.com/keboola/go-client/pkg/keboola"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

type (
	Branch        struct{ PrefixT[definition.Branch] }
	BranchInState Branch
)

func New(s *serde.Serde) Branch {
	return Branch{PrefixT: NewTypedPrefix[definition.Branch]("definition/branch", s)}
}

// Active prefix contains all not deleted objects.
func (v Branch) Active() BranchInState {
	return BranchInState{PrefixT: v.PrefixT.Add("active")}
}

// Deleted prefix contains all deleted objects whose parent existed on deleted.
func (v Branch) Deleted() BranchInState {
	return BranchInState{PrefixT: v.PrefixT.Add("deleted")}
}

func (v BranchInState) InProject(k keboola.ProjectID) PrefixT[definition.Branch] {
	return v.PrefixT.Add(k.String())
}

func (v BranchInState) ByKey(k BranchKey) KeyT[definition.Branch] {
	return v.PrefixT.Key(k.String())
}
