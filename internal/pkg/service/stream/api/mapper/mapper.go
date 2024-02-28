// Package mapper provides bidirectional mapping between API and database model.
package mapper

import "net/url"

type Mapper struct {
	publicURL *url.URL
}

type dependencies interface {
	APIPublicURL() *url.URL
}

func New(d dependencies) *Mapper {
	return &Mapper{
		publicURL: d.APIPublicURL(),
	}
}
