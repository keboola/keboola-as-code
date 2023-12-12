package dependencies

import (
	"context"

	etcdPkg "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// etcdClientScope implements the EtcdClientScope interface.
type etcdClientScope struct {
	client *etcdPkg.Client
	serde  *serde.Serde
}

func NewEtcdClientScope(ctx context.Context, baseScp BaseScope, cfg etcdclient.Config) (EtcdClientScope, error) {
	return newEtcdClientScope(ctx, baseScp, cfg)
}

func newEtcdClientScope(ctx context.Context, baseScp BaseScope, cfg etcdclient.Config) (v *etcdClientScope, err error) {
	ctx, span := baseScp.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.NewEtcdClientScope")
	defer span.End(&err)

	client, err := etcdclient.New(ctx, baseScp.Process(), baseScp.Telemetry(), baseScp.Logger(), cfg)
	if err != nil {
		return nil, err
	}

	return &etcdClientScope{
		client: client,
		serde:  serde.NewJSON(baseScp.Validator().Validate),
	}, nil
}

func (v *etcdClientScope) check() {
	if v == nil {
		panic(errors.New("dependencies etcd client scope is not initialized"))
	}
}

func (v *etcdClientScope) EtcdClient() *etcdPkg.Client {
	v.check()
	return v.client
}

func (v *etcdClientScope) EtcdSerde() *serde.Serde {
	v.check()
	return v.serde
}
