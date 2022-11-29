package schema

import . "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"

type SecretsRoot struct {
	prefix
	schema *Schema
}

func (v *Schema) Secrets() SecretsRoot {
	return SecretsRoot{prefix: NewPrefix("secret"), schema: v}
}
