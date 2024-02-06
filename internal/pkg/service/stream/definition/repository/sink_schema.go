package repository

import (
	"github.com/keboola/go-client/pkg/keboola"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type (
	sinkSchema           struct{ PrefixT[definition.Sink] }
	sinkSchemaInState    sinkSchema
	sinkSchemaVersions   sinkSchema
	sinkSchemaVersionsOf sinkSchema
)

func newSinkSchema(s *serde.Serde) sinkSchema {
	return sinkSchema{PrefixT: NewTypedPrefix[definition.Sink]("definition/sink", s)}
}

// Active prefix contains all not deleted objects.
func (v sinkSchema) Active() sinkSchemaInState {
	return sinkSchemaInState{PrefixT: v.PrefixT.Add("active")}
}

// Deleted prefix contains all deleted objects whose parent existed on deleted.
func (v sinkSchema) Deleted() sinkSchemaInState {
	return sinkSchemaInState{PrefixT: v.PrefixT.Add("deleted")}
}

// Versions prefix contains full history of the object.
func (v sinkSchema) Versions() sinkSchemaVersions {
	return sinkSchemaVersions{PrefixT: v.PrefixT.Add("version")}
}

func (v sinkSchemaInState) In(objectKey any) PrefixT[definition.Sink] {
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

func (v sinkSchemaInState) InProject(k keboola.ProjectID) PrefixT[definition.Sink] {
	return v.PrefixT.Add(k.String())
}

func (v sinkSchemaInState) InBranch(k BranchKey) PrefixT[definition.Sink] {
	return v.PrefixT.Add(k.String())
}

func (v sinkSchemaInState) InSource(k SourceKey) PrefixT[definition.Sink] {
	return v.PrefixT.Add(k.String())
}

func (v sinkSchemaInState) ByKey(k SinkKey) KeyT[definition.Sink] {
	return v.PrefixT.Key(k.String())
}

func (v sinkSchemaVersions) Of(k SinkKey) sinkSchemaVersionsOf {
	return sinkSchemaVersionsOf{PrefixT: v.PrefixT.Add(k.String())}
}

func (v sinkSchemaVersionsOf) Version(version definition.VersionNumber) KeyT[definition.Sink] {
	return v.PrefixT.Key(version.String())
}
