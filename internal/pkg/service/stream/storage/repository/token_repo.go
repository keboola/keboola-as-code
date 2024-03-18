package repository

import (
	"fmt"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	etcd "go.etcd.io/etcd/client/v3"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository/schema"
)

// TokenRepository provides database operations with the storage.Token entity.
// The orchestration of these database operations with other parts of the platform is handled by an upper facade.
type TokenRepository struct {
	clock  clock.Clock
	client etcd.KV
	schema schema.Token
	all    *Repository
}

func newTokenRepository(d dependencies, all *Repository) *TokenRepository {
	return &TokenRepository{
		clock:  d.Clock(),
		client: d.EtcdClient(),
		schema: schema.ForToken(d.EtcdSerde()),
		all:    all,
	}
}

// List lists tokens in the parent.
func (r *TokenRepository) List(parentKey fmt.Stringer) iterator.DefinitionT[model.Token] {
	return r.schema.InObject(parentKey).GetAll(r.client)
}

func (r *TokenRepository) Put(k key.SinkKey, token keboola.Token) op.WithResult[model.Token] {
	result := model.Token{SinkKey: k, Token: token}
	return r.schema.ByKey(k).Put(r.client, result)
}

func (r *TokenRepository) GetKV(k key.SinkKey) op.WithResult[*op.KeyValueT[model.Token]] {
	return r.schema.ByKey(k).GetKV(r.client)
}

func (r *TokenRepository) Get(k key.SinkKey) op.WithResult[model.Token] {
	return r.schema.ByKey(k).Get(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink token", k.String(), "database")
		})
}

func (r *TokenRepository) Delete(k key.SinkKey) op.BoolOp {
	return r.schema.ByKey(k).DeleteIfExists(r.client).
		WithEmptyResultAsError(func() error {
			return serviceError.NewResourceNotFoundError("sink token", k.String(), "database")
		})
}

func (r *TokenRepository) DeleteAll(parentKey fmt.Stringer) op.CountOp {
	return r.schema.InObject(parentKey).DeleteAll(r.client)
}
