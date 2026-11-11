package apptoken_test

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
)

// The expected key was computed with OpenSSL, independently of this code:
// openssl kdf -keylen 32 -kdfopt digest:SHA256 -kdfopt hexkey:<private scalar> -kdfopt info:"kbc app-user-token refresh v1" HKDF
// Changing the derivation invalidates every refresh token issued before the deploy.
const (
	refreshKeyVectorPEM = `-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIHmx7aWfVfVlggD/nWAg/Ga1PigAClBQ1hm1wMUG635VoAoGCCqGSM49
AwEHoUQDQgAEZRbRMjGoBXkRSjvDRrTIbSb+Dyy0Vz7ubkZ4WhR1GBjgf3ro6k1A
gEDErG3X7HGF3dHkcYt7G8FFpnXo289CxQ==
-----END EC PRIVATE KEY-----
`
	refreshKeyVector = "d9f828ead6cbd5c646ed4215e6592f1c1759fc347f5f922313141586754271b7"
)

func TestDeriveRefreshKey_TestVector(t *testing.T) {
	t.Parallel()
	block, _ := pem.Decode([]byte(refreshKeyVectorPEM))
	require.NotNil(t, block)
	key, err := x509.ParseECPrivateKey(block.Bytes)
	require.NoError(t, err)

	derived, err := apptoken.DeriveRefreshKey(key)
	require.NoError(t, err)
	assert.Equal(t, refreshKeyVector, hex.EncodeToString(derived))
}

func TestDeriveRefreshKey_DiffersPerSigningKey(t *testing.T) {
	t.Parallel()
	keys := []*ecdsa.PrivateKey{newTestKey(t), newTestKey(t)}
	a, err := apptoken.DeriveRefreshKey(keys[0])
	require.NoError(t, err)
	b, err := apptoken.DeriveRefreshKey(keys[1])
	require.NoError(t, err)
	assert.Len(t, a, 32)
	assert.NotEqual(t, a, b)
}
