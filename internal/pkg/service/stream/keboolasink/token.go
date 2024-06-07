package keboolasink

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// Token is a Storage API Token used to create staging files and import files to the target table.
// The token is scoped only to the target table, so there is a separate token for each definition.TableSink.
type Token struct {
	key.SinkKey
	Token keboola.Token `json:"token"`
}

func (v Token) TokenString() string {
	return v.Token.Token
}
