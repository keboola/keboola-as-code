package apptoken

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
)

type Minter struct {
	keyring *Keyring
}

type dependencies interface {
	Config() config.Config
}

func NewMinter(d dependencies) (*Minter, error) {
	cfg := d.Config().AppUserToken
	keyring, err := NewKeyring(cfg.SigningKeys, cfg.ActiveKid)
	if err != nil {
		return nil, err
	}
	return &Minter{keyring: keyring}, nil
}

func (m *Minter) JWKS() []byte {
	return m.keyring.JWKS()
}
