package preview

import (
	"context"

	"github.com/jonboulle/clockwork"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/jwks"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/session"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Dependencies interface {
	Logger() log.Logger
	Clock() clockwork.Clock
	Process() *servicectx.Process
}

type Service struct {
	links          *LinkVerifier
	sessions       *session.Manager
	frameAncestors []string
}

func New(d Dependencies, cfg config.Preview) *Service {
	logger := d.Logger().WithComponent("preview")
	keys := jwks.New(jwks.Config{
		URL:             cfg.JWKSURL,
		RefreshInterval: jwks.RefreshInterval,
		MaxStaleness:    jwks.MaxStaleness,
	}, d.Clock(), logger)

	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(context.Context) {
		cancel(errors.New("shutdown"))
	})
	go keys.Run(ctx)

	return &Service{
		links:          NewLinkVerifier(keys, cfg.Issuer, d.Clock()),
		sessions:       session.NewManager(cfg.SessionSigningKey, session.IdleTTL, session.MaxTTL, d.Clock()),
		frameAncestors: cfg.AllowedFrameAncestors,
	}
}

func (s *Service) Links() *LinkVerifier       { return s.links }
func (s *Service) Sessions() *session.Manager { return s.sessions }
func (s *Service) FrameAncestors() []string   { return s.frameAncestors }
