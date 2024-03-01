// Package mapper provides bidirectional mapping between API and database model.
package mapper

import "net/url"

type Mapper struct {
	apiPublicURL        *url.URL
	httpSourcePublicURL *url.URL
}

type dependencies interface {
	APIPublicURL() *url.URL
	HTTPSourcePublicURL() *url.URL
}

func New(d dependencies) *Mapper {
	return &Mapper{
		apiPublicURL:        d.APIPublicURL(),
		httpSourcePublicURL: d.HTTPSourcePublicURL(),
	}
}
