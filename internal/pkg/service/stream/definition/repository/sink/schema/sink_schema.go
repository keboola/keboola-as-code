package schema

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type (
	Sink struct {
		etcdop.PrefixT[definition.Sink]
	}
	SinkInState    Sink
	SinkVersions   Sink
	SinkVersionsOf Sink
)

func New(s *serde.Serde) Sink {
	return Sink{PrefixT: etcdop.NewTypedPrefix[definition.Sink]("definition/sink", s)}
}

// Active prefix contains all not deleted objects.
func (v Sink) Active() SinkInState {
	return SinkInState{PrefixT: v.Add("active")}
}

// Deleted prefix contains all deleted objects whose parent existed on deleted.
func (v Sink) Deleted() SinkInState {
	return SinkInState{PrefixT: v.Add("deleted")}
}

// Versions prefix contains full history of the object.
func (v Sink) Versions() SinkVersions {
	return SinkVersions{PrefixT: v.Add("version")}
}

func (v SinkInState) In(objectKey any) etcdop.PrefixT[definition.Sink] {
	switch k := objectKey.(type) {
	case keboola.ProjectID:
		return v.InProject(k)
	case key.BranchKey:
		return v.InBranch(k)
	case key.SourceKey:
		return v.InSource(k)
	default:
		panic(errors.Errorf(`unexpected Sink parent key type "%T"`, objectKey))
	}
}

func (v SinkInState) InProject(k keboola.ProjectID) etcdop.PrefixT[definition.Sink] {
	return v.Add(k.String())
}

func (v SinkInState) InBranch(k key.BranchKey) etcdop.PrefixT[definition.Sink] {
	return v.Add(k.String())
}

func (v SinkInState) InSource(k key.SourceKey) etcdop.PrefixT[definition.Sink] {
	return v.Add(k.String())
}

func (v SinkInState) ByKey(k key.SinkKey) etcdop.KeyT[definition.Sink] {
	return v.Key(k.String())
}

func (v SinkVersions) Of(k key.SinkKey) SinkVersionsOf {
	return SinkVersionsOf{PrefixT: v.Add(k.String())}
}

func (v SinkVersionsOf) Version(version definition.VersionNumber) etcdop.KeyT[definition.Sink] {
	return v.Key(version.String())
}
