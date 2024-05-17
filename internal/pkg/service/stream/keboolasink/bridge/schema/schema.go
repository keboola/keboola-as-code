package schema

import "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"

type Schema struct {
	token             Token
	uploadCredentials UploadCredentials
}

func New(s *serde.Serde) Schema {
	return Schema{
		token:             forToken(s),
		uploadCredentials: forFileUploadCredentials(s),
	}
}

func (s Schema) Token() Token {
	return s.token
}

func (s Schema) UploadCredentials() UploadCredentials {
	return s.uploadCredentials
}
