package schema

import (
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type (
	// Token is an etcd prefix that stores all token entities.
	Token         struct{ etcdop.PrefixT[model.Token] }
	TokenInObject Token
)

func ForToken(s *serde.Serde) Token {
	return Token{PrefixT: etcdop.NewTypedPrefix[model.Token]("storage/secret/token", s)}
}

func (v Token) ByKey(k key.SinkKey) etcdop.KeyT[model.Token] {
	return v.PrefixT.Key(k.String())
}

func (v Token) InObject(k fmt.Stringer) TokenInObject {
	switch k.(type) {
	case keboola.ProjectID, key.BranchKey, key.SourceKey:
		return v.inObject(k)
	default:
		panic(errors.Errorf(`unexpected object key "%T"`, k))
	}
}

func (v Token) InProject(projectID keboola.ProjectID) TokenInObject {
	return v.inObject(projectID)
}

func (v Token) InBranch(k key.BranchKey) TokenInObject {
	return v.inObject(k)
}

func (v Token) InSource(k key.SourceKey) TokenInObject {
	return v.inObject(k)
}

func (v Token) inObject(objectKey fmt.Stringer) TokenInObject {
	return TokenInObject{PrefixT: v.PrefixT.Add(objectKey.String())}
}
