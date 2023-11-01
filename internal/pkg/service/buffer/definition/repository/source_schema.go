package repository

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type (
	sourceSchema           struct{ PrefixT[definition.Source] }
	sourceSchemaInState    sourceSchema
	sourceSchemaVersions   sourceSchema
	sourceSchemaVersionsOf sourceSchema
)

func newSourceSchema(s *serde.Serde) sourceSchema {
	return sourceSchema{PrefixT: NewTypedPrefix[definition.Source]("definition/source", s)}
}

// Active prefix contains all not deleted objects.
func (v sourceSchema) Active() sourceSchemaInState {
	return sourceSchemaInState{PrefixT: v.PrefixT.Add("active")}
}

// Deleted prefix contains all deleted objects whose parent existed on deleted.
func (v sourceSchema) Deleted() sourceSchemaInState {
	return sourceSchemaInState{PrefixT: v.PrefixT.Add("deleted")}
}

// Versions prefix contains full history of the object.
func (v sourceSchema) Versions() sourceSchemaVersions {
	return sourceSchemaVersions{PrefixT: v.PrefixT.Add("version")}
}

func (v sourceSchemaInState) In(objectKey any) PrefixT[definition.Source] {
	switch k := objectKey.(type) {
	case keboola.ProjectID:
		return v.InProject(k)
	case BranchKey:
		return v.InBranch(k)
	default:
		panic(errors.Errorf(`unexpected Source parent key type "%T"`, objectKey))
	}
}

func (v sourceSchemaInState) InProject(k keboola.ProjectID) PrefixT[definition.Source] {
	return v.PrefixT.Add(k.String())
}

func (v sourceSchemaInState) InBranch(k BranchKey) PrefixT[definition.Source] {
	return v.PrefixT.Add(k.String())
}

func (v sourceSchemaInState) ByKey(k SourceKey) KeyT[definition.Source] {
	return v.PrefixT.Key(k.String())
}

func (v sourceSchemaVersions) Of(k SourceKey) sourceSchemaVersionsOf {
	return sourceSchemaVersionsOf{PrefixT: v.PrefixT.Add(k.String())}
}

func (v sourceSchemaVersionsOf) Version(version definition.VersionNumber) KeyT[definition.Source] {
	return v.PrefixT.Key(version.String())
}
