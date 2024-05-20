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
	Sink           struct{ PrefixT[definition.Sink] }
	SinkInState    Sink
	SinkVersions   Sink
	SinkVersionsOf Sink
)

func New(s *serde.Serde) Sink {
	return Sink{PrefixT: NewTypedPrefix[definition.Sink]("definition/sink", s)}
}

// Active prefix contains all not deleted objects.
func (v Sink) Active() SinkInState {
	return SinkInState{PrefixT: v.PrefixT.Add("active")}
}

// Deleted prefix contains all deleted objects whose parent existed on deleted.
func (v Sink) Deleted() SinkInState {
	return SinkInState{PrefixT: v.PrefixT.Add("deleted")}
}

// Versions prefix contains full history of the object.
func (v Sink) Versions() SinkVersions {
	return SinkVersions{PrefixT: v.PrefixT.Add("version")}
}

func (v SinkInState) In(objectKey any) PrefixT[definition.Sink] {
	switch k := objectKey.(type) {
	case keboola.ProjectID:
		return v.InProject(k)
	case BranchKey:
		return v.InBranch(k)
	case SourceKey:
		return v.InSource(k)
	default:
		panic(errors.Errorf(`unexpected Sink parent key type "%T"`, objectKey))
	}
}

func (v SinkInState) InProject(k keboola.ProjectID) PrefixT[definition.Sink] {
	return v.PrefixT.Add(k.String())
}

func (v SinkInState) InBranch(k BranchKey) PrefixT[definition.Sink] {
	return v.PrefixT.Add(k.String())
}

func (v SinkInState) InSource(k SourceKey) PrefixT[definition.Sink] {
	return v.PrefixT.Add(k.String())
}

func (v SinkInState) ByKey(k SinkKey) KeyT[definition.Sink] {
	return v.PrefixT.Key(k.String())
}

func (v SinkVersions) Of(k SinkKey) SinkVersionsOf {
	return SinkVersionsOf{PrefixT: v.PrefixT.Add(k.String())}
}

func (v SinkVersionsOf) Version(version definition.VersionNumber) KeyT[definition.Sink] {
	return v.PrefixT.Key(version.String())
}
