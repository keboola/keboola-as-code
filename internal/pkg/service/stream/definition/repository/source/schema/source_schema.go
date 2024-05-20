package schema

import (
	"github.com/keboola/go-client/pkg/keboola"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type (
	Source           struct{ PrefixT[definition.Source] }
	SourceInState    Source
	SourceVersions   Source
	SourceVersionsOf Source
)

func New(s *serde.Serde) Source {
	return Source{PrefixT: NewTypedPrefix[definition.Source]("definition/source", s)}
}

// Active prefix contains all not deleted objects.
func (v Source) Active() SourceInState {
	return SourceInState{PrefixT: v.PrefixT.Add("active")}
}

// Deleted prefix contains all deleted objects whose parent existed on deleted.
func (v Source) Deleted() SourceInState {
	return SourceInState{PrefixT: v.PrefixT.Add("deleted")}
}

// Versions prefix contains full history of the object.
func (v Source) Versions() SourceVersions {
	return SourceVersions{PrefixT: v.PrefixT.Add("version")}
}

func (v SourceInState) In(objectKey any) PrefixT[definition.Source] {
	switch k := objectKey.(type) {
	case keboola.ProjectID:
		return v.InProject(k)
	case BranchKey:
		return v.InBranch(k)
	default:
		panic(errors.Errorf(`unexpected Source parent key type "%T"`, objectKey))
	}
}

func (v SourceInState) InProject(k keboola.ProjectID) PrefixT[definition.Source] {
	return v.PrefixT.Add(k.String())
}

func (v SourceInState) InBranch(k BranchKey) PrefixT[definition.Source] {
	return v.PrefixT.Add(k.String())
}

func (v SourceInState) ByKey(k SourceKey) KeyT[definition.Source] {
	return v.PrefixT.Key(k.String())
}

func (v SourceVersions) Of(k SourceKey) SourceVersionsOf {
	return SourceVersionsOf{PrefixT: v.PrefixT.Add(k.String())}
}

func (v SourceVersionsOf) Version(version definition.VersionNumber) KeyT[definition.Source] {
	return v.PrefixT.Key(version.String())
}
