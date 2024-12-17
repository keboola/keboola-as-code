package keboola

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Token is a Storage API Token used to create staging files and import files to the target table.
// The token is scoped only to the target table, so there is a separate token for each definition.TableSink.
type Token struct {
	key.SinkKey
	Token          *keboola.Token `json:"token"`
	TokenID        string         `json:"tokenId"`
	EncryptedToken []byte         `json:"encryptedToken"`
}

func (token Token) ID() string {
	if token.EncryptedToken != nil {
		return token.TokenID
	}
	return token.Token.ID
}

func (token Token) DecryptToken(ctx context.Context, encryptor *cloudencrypt.GenericEncryptor[keboola.Token], metadata cloudencrypt.Metadata) (keboola.Token, error) {
	if token.EncryptedToken != nil {
		if encryptor == nil {
			return keboola.Token{}, errors.New("missing token encryptor")
		}

		return encryptor.Decrypt(ctx, token.EncryptedToken, metadata)
	}

	return *token.Token, nil
}
