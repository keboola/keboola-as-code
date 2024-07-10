package dependencies

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	sinkRouter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/router"
)

// sourceScope implements SourceScope interface.
type sourceScope struct {
	sourceParentScopes
	sinkRouter sinkRouter.Router
}

type sourceParentScopes interface {
	ServiceScope
}

type sourceParentScopesImpl struct {
	ServiceScope
}

func (v *sourceScope) SinkRouter() sinkRouter.Router {
	return v.sinkRouter
}

func NewSourceScope(d sourceParentScopes, cfg config.Config) (v SourceScope, err error) {
	return newSourceScope(d, cfg)
}

func newSourceScope(parentScp sourceParentScopes, _ config.Config) (v SourceScope, err error) {
	d := &sourceScope{}

	d.sourceParentScopes = parentScp

	d.sinkRouter, err = sinkRouter.New(d)
	if err != nil {
		return nil, err
	}

	return d, nil
}
