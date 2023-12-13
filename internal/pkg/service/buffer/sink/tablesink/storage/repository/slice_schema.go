package repository

import (
	"fmt"
	"github.com/keboola/go-client/pkg/keboola"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type (
	// sliceSchema is an etcd prefix that stores all slice entities.
	sliceSchema struct{ PrefixT[Slice] }
	// sliceSchemaInLevel - see sliceSchema.AllLevels and sliceSchema.InLevel methods.
	// The entity is stored in 2 copies, under "All" prefix and "InLevel" prefix.
	// - "All" prefix is used for classic CRUD operations.
	// - "InLevel" prefix is used for effective watching of the storage level.
	sliceSchemaInLevel  sliceSchema
	sliceSchemaInObject sliceSchema
)

func newSliceSchema(s *serde.Serde) sliceSchema {
	return sliceSchema{PrefixT: NewTypedPrefix[Slice]("storage/slice", s)}
}

func (s sliceSchema) AllLevels() sliceSchemaInLevel {
	return sliceSchemaInLevel{PrefixT: s.PrefixT.Add("all")}
}

func (s sliceSchema) InLevel(level Level) sliceSchemaInLevel {
	switch level {
	case LevelLocal, LevelStaging, LevelTarget:
		return sliceSchemaInLevel{PrefixT: s.PrefixT.Add("level").Add(level.String())}
	default:
		panic(errors.Errorf(`unexpected storage level "%v"`, level))
	}
}

func (v sliceSchemaInLevel) ByKey(k SliceKey) KeyT[Slice] {
	return v.PrefixT.Key(k.String())
}

func (v sliceSchemaInLevel) InObject(k fmt.Stringer) sliceSchemaInObject {
	switch k.(type) {
	case keboola.ProjectID, BranchKey, SourceKey, SinkKey, FileKey:
		return v.inObject(k)
	default:
		panic(errors.Errorf(`unexpected object key "%T"`, k))
	}
}

func (v sliceSchemaInLevel) InProject(projectID keboola.ProjectID) sliceSchemaInObject {
	return v.inObject(projectID)
}

func (v sliceSchemaInLevel) InBranch(k BranchKey) sliceSchemaInObject {
	return v.inObject(k)
}

func (v sliceSchemaInLevel) InSource(k SourceKey) sliceSchemaInObject {
	return v.inObject(k)
}

func (v sliceSchemaInLevel) InSink(k SinkKey) sliceSchemaInObject {
	return v.inObject(k)
}

func (v sliceSchemaInLevel) InFile(k FileKey) sliceSchemaInObject {
	return v.inObject(k)
}

func (v sliceSchemaInLevel) inObject(objectKey fmt.Stringer) sliceSchemaInObject {
	return sliceSchemaInObject{PrefixT: v.PrefixT.Add(objectKey.String())}
}
