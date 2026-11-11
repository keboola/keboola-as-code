package apptoken

import (
	"crypto/ecdsa"
	"crypto/hkdf"
	"crypto/sha256"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	refreshKeyInfo  = "kbc app-user-token refresh v1"
	refreshKeyBytes = 32
)

// DeriveRefreshKey derives the HS256 key of refresh tokens from an app user token signing key.
// A change of the derivation invalidates every refresh token already issued.
func DeriveRefreshKey(key *ecdsa.PrivateKey) ([]byte, error) {
	scalar, err := key.Bytes()
	if err != nil {
		return nil, errors.PrefixError(err, "app user token: cannot read the signing key")
	}
	derived, err := hkdf.Key(sha256.New, scalar, nil, refreshKeyInfo, refreshKeyBytes)
	if err != nil {
		return nil, errors.PrefixError(err, "app user token: cannot derive the refresh key")
	}
	return derived, nil
}
