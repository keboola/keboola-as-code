package dependencies

import (
	"context"

	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// encryptionScope implements EncryptionScope interface.
type encryptionScope struct {
	encryptor cloudencrypt.Encryptor
}

type encryptionScopeDeps interface {
	BaseScope
	EtcdClientScope
}

func NewEncryptionScope(ctx context.Context, cfg encryption.Config, d encryptionScopeDeps) (EncryptionScope, error) {
	return newEncryptionScope(ctx, cfg, d)
}

func newEncryptionScope(ctx context.Context, cfg encryption.Config, d encryptionScopeDeps) (v *encryptionScope, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.NewEncryptionScope")
	defer span.End(&err)

	encryptor, err := encryption.NewEncryptor(ctx, cfg)
	if err != nil {
		return nil, err
	}

	return &encryptionScope{encryptor: encryptor}, nil
}

func (v *encryptionScope) check() {
	if v == nil {
		panic(errors.New("dependencies encryption scope is not initialized"))
	}
}

func (v *encryptionScope) Encryptor() cloudencrypt.Encryptor {
	v.check()
	return v.encryptor
}
