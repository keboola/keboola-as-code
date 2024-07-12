// Package mapper provides bidirectional mapping between API and database model.
package mapper

import (
	"net/url"

	jsonnetWrapper "github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/receive/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/receive/receivectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
)

type Mapper struct {
	config              config.Config
	apiPublicURL        *url.URL
	httpSourcePublicURL *url.URL
	jsonnetPool         *jsonnetWrapper.VMPool[receivectx.Context]
}

type dependencies interface {
	APIPublicURL() *url.URL
	HTTPSourcePublicURL() *url.URL
}

func New(d dependencies, cfg config.Config) *Mapper {
	return &Mapper{
		config:              cfg,
		apiPublicURL:        d.APIPublicURL(),
		httpSourcePublicURL: d.HTTPSourcePublicURL(),
		jsonnetPool:         jsonnet.NewPool(),
	}
}
