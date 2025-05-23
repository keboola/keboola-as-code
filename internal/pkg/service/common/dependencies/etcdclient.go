package dependencies

import (
	"context"
	"reflect"

	etcdPkg "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	etcdserde "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// etcdClientScope implements the EtcdClientScope interface.
type etcdClientScope struct {
	client *etcdPkg.Client
	serde  *etcdserde.Serde
}

func NewEtcdClientScope(ctx context.Context, baseScp BaseScope, cfg etcdclient.Config) (EtcdClientScope, error) {
	return newEtcdClientScope(ctx, baseScp, cfg)
}

func newEtcdClientScope(ctx context.Context, baseScp BaseScope, cfg etcdclient.Config) (v *etcdClientScope, err error) {
	ctx, span := baseScp.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.NewEtcdClientScope")
	defer span.End(&err)

	client, err := etcdclient.New(ctx, baseScp.Process(), baseScp.Telemetry(), baseScp.Logger(), baseScp.Stderr(), cfg)
	if err != nil {
		return nil, err
	}

	validate := func(ctx context.Context, value any) error {
		if k := reflect.ValueOf(value).Kind(); k != reflect.Struct && k != reflect.Pointer {
			return baseScp.Validator().Validate(ctx, value)
		}
		return nil
	}

	var serde *etcdserde.Serde
	if cfg.JSONNumbers {
		serde = etcdserde.NewJSONWithNumbers(validate)
	} else {
		serde = etcdserde.NewJSON(validate)
	}

	return &etcdClientScope{
		client: client,
		serde:  serde,
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

func (v *etcdClientScope) EtcdSerde() *etcdserde.Serde {
	v.check()
	return v.serde
}
