package file

import (
	"net/http"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
)

const (
	gzipLevel = 2 // 1 - BestSpeed, 9 - BestCompression
)

type Manager struct {
	clock     clock.Clock
	store     *store.Store
	publicAPI *keboola.API
	transport http.RoundTripper
}

type manager = Manager

type AuthorizedManager struct {
	*manager
	projectAPI *keboola.API
}

type dependencies interface {
	Clock() clock.Clock
	Store() *store.Store
	ServiceConfig() config.ServiceConfig
	KeboolaPublicAPI() *keboola.API
}

func NewManager(d dependencies) *Manager {
	return &Manager{
		clock:     d.Clock(),
		store:     d.Store(),
		publicAPI: d.KeboolaPublicAPI(),
		transport: d.ServiceConfig().UploadTransport,
	}
}

func (m *Manager) WithToken(token string) *AuthorizedManager {
	return &AuthorizedManager{manager: m, projectAPI: m.publicAPI.WithToken(token)}
}
