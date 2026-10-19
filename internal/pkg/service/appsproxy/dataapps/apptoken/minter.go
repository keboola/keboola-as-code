package apptoken

import (
	"context"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Minter struct {
	keyring *Keyring
	issuer  string
	ttl     time.Duration
	clock   clockwork.Clock
	logger  log.Logger
	apps    appStates
	metrics *metrics
}

type appStates interface {
	GetState(ctx context.Context, ref k8sapp.WorkloadRef) (k8sapp.AppInfo, bool)
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	Config() config.Config
	Telemetry() telemetry.Telemetry
	AppStateWatcher() *k8sapp.StateWatcher
}

func NewMinter(d dependencies) (*Minter, error) {
	cfg := d.Config()
	keyring, err := NewKeyring(cfg.AppUserToken.SigningKeys, cfg.AppUserToken.ActiveKid)
	if err != nil {
		return nil, err
	}
	return &Minter{
		keyring: keyring,
		issuer:  cfg.API.PublicURL.String(),
		ttl:     cfg.AppUserToken.TTL,
		clock:   d.Clock(),
		logger:  d.Logger().WithComponent("app-user-token"),
		apps:    d.AppStateWatcher(),
		metrics: newMetrics(d.Telemetry().Meter()),
	}, nil
}

func (m *Minter) JWKS() []byte {
	return m.keyring.JWKS()
}
