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
	// fileSchema is an etcd prefix that stores all file entities.
	fileSchema struct{ PrefixT[File] }
	// fileSchemaInLevel - see fileSchema.AllLevels and fileSchema.InLevel methods.
	// The entity is stored in 2 copies, under "All" prefix and "InLevel" prefix.
	// - "All" prefix is used for classic CRUD operations.
	// - "InLevel" prefix is used for effective watching of the storage level.
	fileSchemaInLevel  fileSchema
	fileSchemaInObject fileSchema
)

func newFileSchema(s *serde.Serde) fileSchema {
	return fileSchema{PrefixT: NewTypedPrefix[File]("storage/file", s)}
}

func (s fileSchema) AllLevels() fileSchemaInLevel {
	return fileSchemaInLevel{PrefixT: s.PrefixT.Add("all")}
}

func (s fileSchema) InLevel(level Level) fileSchemaInLevel {
	switch level {
	case LevelLocal, LevelStaging, LevelTarget:
		return fileSchemaInLevel{PrefixT: s.PrefixT.Add("level").Add(level.String())}
	default:
		panic(errors.Errorf(`unexpected storage level "%v"`, level))
	}
}

func (v fileSchemaInLevel) ByKey(k FileKey) KeyT[File] {
	return v.PrefixT.Key(k.String())
}

func (v fileSchemaInLevel) InObject(k fmt.Stringer) fileSchemaInObject {
	switch k.(type) {
	case keboola.ProjectID, BranchKey, SourceKey, SinkKey:
		return v.inObject(k)
	default:
		panic(errors.Errorf(`unexpected object key "%T"`, k))
	}
}

func (v fileSchemaInLevel) InProject(projectID keboola.ProjectID) fileSchemaInObject {
	return v.inObject(projectID)
}

func (v fileSchemaInLevel) InBranch(k BranchKey) fileSchemaInObject {
	return v.inObject(k)
}

func (v fileSchemaInLevel) InSource(k SourceKey) fileSchemaInObject {
	return v.inObject(k)
}

func (v fileSchemaInLevel) InSink(k SinkKey) fileSchemaInObject {
	return v.inObject(k)
}

func (v fileSchemaInLevel) inObject(objectKey fmt.Stringer) fileSchemaInObject {
	return fileSchemaInObject{PrefixT: v.PrefixT.Add(objectKey.String())}
}
