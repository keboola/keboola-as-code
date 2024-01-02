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
	// tokenSchema is an etcd prefix that stores all token entities.
	tokenSchema         struct{ PrefixT[Token] }
	tokenSchemaInObject tokenSchema
)

func newTokenSchema(s *serde.Serde) tokenSchema {
	return tokenSchema{PrefixT: NewTypedPrefix[Token]("storage/secret/token", s)}
}

func (v tokenSchema) ByKey(k SinkKey) KeyT[Token] {
	return v.PrefixT.Key(k.String())
}

func (v tokenSchema) InObject(k fmt.Stringer) tokenSchemaInObject {
	switch k.(type) {
	case keboola.ProjectID, BranchKey, SourceKey:
		return v.inObject(k)
	default:
		panic(errors.Errorf(`unexpected object key "%T"`, k))
	}
}

func (v tokenSchema) InProject(projectID keboola.ProjectID) tokenSchemaInObject {
	return v.inObject(projectID)
}

func (v tokenSchema) InBranch(k BranchKey) tokenSchemaInObject {
	return v.inObject(k)
}

func (v tokenSchema) InSource(k SourceKey) tokenSchemaInObject {
	return v.inObject(k)
}

func (v tokenSchema) inObject(objectKey fmt.Stringer) tokenSchemaInObject {
	return tokenSchemaInObject{PrefixT: v.PrefixT.Add(objectKey.String())}
}
