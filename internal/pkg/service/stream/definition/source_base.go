package definition

import (
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

type SourceType string

type Source struct {
	key.SourceKey
	Created
	Versioned
	Switchable
	SoftDeletable
	Type        SourceType           `json:"type" validate:"required"`
	Name        string               `json:"name" validate:"required,min=1,max=40"`
	Description string               `json:"description,omitempty" validate:"max=4096"`
	Config      configpatch.PatchKVs `json:"config,omitempty"` // see stream/config/config.Patch

	// Source type specific fields

	HTTP *HTTPSource `json:"http,omitempty" validate:"required_if=Type http"`
}

func (s *Source) FormatHTTPSourceURL(httpSourcePublicURL string) (string, error) {
	u, err := url.Parse(httpSourcePublicURL)
	if err != nil {
		return "", err
	}

	return u.JoinPath("stream", s.ProjectID.String(), s.SourceID.String(), s.HTTP.Secret).String(), nil
}
