package apptoken_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/preview/jwks"
)

type testKey struct {
	Kid string `json:"kid"`
	Pem string `json:"pem"`
}

func newTestKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return key
}

func ecPEM(t *testing.T, key *ecdsa.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}))
}

func pkcs8PEM(t *testing.T, key any) string {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

func keysJSON(t *testing.T, keys ...testKey) string {
	t.Helper()
	out, err := json.Marshal(keys)
	require.NoError(t, err)
	return string(out)
}

// pemBodySample is a line from the middle of the PEM body, so an error that
// echoes the key without its BEGIN line is still detected.
func pemBodySample(p string) string {
	return strings.Split(p, "\n")[2]
}

type jwk struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func decodeCoordinate(t *testing.T, v string) []byte {
	t.Helper()
	b, err := base64.RawURLEncoding.DecodeString(v)
	require.NoError(t, err)
	require.Len(t, b, 32)
	return b
}

// coordinates returns X and Y, each 32 bytes, from the uncompressed point encoding 0x04 || X || Y.
func coordinates(t *testing.T, key *ecdsa.PublicKey) ([]byte, []byte) {
	t.Helper()
	point, err := key.Bytes()
	require.NoError(t, err)
	return point[1:33], point[33:]
}

func publicKeysFromJWKS(t *testing.T, doc []byte) map[string]*ecdsa.PublicKey {
	t.Helper()
	var set struct {
		Keys []jwk `json:"keys"`
	}
	require.NoError(t, json.Unmarshal(doc, &set))
	out := make(map[string]*ecdsa.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		point := append([]byte{4}, decodeCoordinate(t, k.X)...)
		point = append(point, decodeCoordinate(t, k.Y)...)
		pub, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), point)
		require.NoError(t, err)
		out[k.Kid] = pub
	}
	return out
}

func TestNewKeyring_Empty(t *testing.T) {
	t.Parallel()

	k, err := apptoken.NewKeyring("", "")
	require.NoError(t, err)
	assert.False(t, k.Configured())
	assert.Empty(t, k.ActiveKid())
	assert.Nil(t, k.ActiveKey())
	assert.JSONEq(t, `{"keys":[]}`, string(k.JWKS()))
}

func TestNewKeyring_Invalid(t *testing.T) {
	t.Parallel()

	k1 := ecPEM(t, newTestKey(t))
	k2 := ecPEM(t, newTestKey(t))
	p384, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	require.NoError(t, err)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	cases := []struct {
		name      string
		keys      string
		activeKid string
		secret    string
		wantErr   string
	}{
		{name: "raw PEM instead of JSON", keys: k1, activeKid: "k1", secret: k1, wantErr: "not a JSON list"},
		{name: "JSON object instead of list", keys: `{"kid":"k1","pem":` + mustJSON(t, k1) + `}`, activeKid: "k1", secret: k1, wantErr: "not a JSON list"},
		{name: "trailing bracket after the list", keys: keysJSON(t, testKey{Kid: "k1", Pem: k1}) + "]", activeKid: "k1", secret: k1, wantErr: "not a JSON list"},
		{name: "second list after the list", keys: keysJSON(t, testKey{Kid: "k1", Pem: k1}) + "[]", activeKid: "k1", secret: k1, wantErr: "not a JSON list"},
		{name: "two keys in one pem", keys: keysJSON(t, testKey{Kid: "k1", Pem: k1 + k2}), activeKid: "k1", secret: k2, wantErr: `signingKeys[0] (kid "k1"): more than one PEM block`},
		{name: "entry without pem", keys: `[{"kid":"k1"}]`, activeKid: "k1", wantErr: "signingKeys[0]: pem is empty"},
		{name: "entry with unknown field", keys: `[{"kid":"k1","pem":` + mustJSON(t, k1) + `,"foo":1}]`, activeKid: "k1", secret: k1, wantErr: "not a JSON list"},
		{name: "invalid kid", keys: keysJSON(t, testKey{Kid: "bad kid!", Pem: k1}), activeKid: "k1", secret: k1, wantErr: "signingKeys[0]: invalid kid"},
		{name: "kid and pem swapped", keys: keysJSON(t, testKey{Kid: k1, Pem: "k1"}), activeKid: "k1", secret: k1, wantErr: "signingKeys[0]: invalid kid"},
		{name: "too long kid", keys: keysJSON(t, testKey{Kid: strings.Repeat("a", 65), Pem: k1}), activeKid: "k1", secret: k1, wantErr: "signingKeys[0]: invalid kid"},
		{name: "duplicate kid", keys: keysJSON(t, testKey{Kid: "k1", Pem: k1}, testKey{Kid: "k1", Pem: k2}), activeKid: "k1", secret: k2, wantErr: `signingKeys[1]: duplicate kid "k1"`},
		{name: "P-384 key", keys: keysJSON(t, testKey{Kid: "k1", Pem: pkcs8PEM(t, p384)}), activeKid: "k1", secret: pkcs8PEM(t, p384), wantErr: `signingKeys[0] (kid "k1"): not an EC P-256 key`},
		{name: "P-384 key in SEC1 format", keys: keysJSON(t, testKey{Kid: "k1", Pem: ecPEM(t, p384)}), activeKid: "k1", secret: ecPEM(t, p384), wantErr: `signingKeys[0] (kid "k1"): not an EC P-256 key`},
		{name: "RSA key", keys: keysJSON(t, testKey{Kid: "k1", Pem: pkcs8PEM(t, rsaKey)}), activeKid: "k1", secret: pkcs8PEM(t, rsaKey), wantErr: `signingKeys[0] (kid "k1"): not an EC P-256 key`},
		{name: "garbage PEM", keys: keysJSON(t, testKey{Kid: "k1", Pem: "-----BEGIN EC PRIVATE KEY-----\nZm9vYmFyYmF6\n-----END EC PRIVATE KEY-----\n"}), activeKid: "k1", wantErr: `signingKeys[0] (kid "k1"): not a parsable private key`},
		{name: "not a PEM", keys: keysJSON(t, testKey{Kid: "k1", Pem: "hello"}), activeKid: "k1", wantErr: `signingKeys[0] (kid "k1"): no PEM block`},
		{name: "active kid not in list", keys: keysJSON(t, testKey{Kid: "k1", Pem: k1}), activeKid: "k2", secret: k1, wantErr: `activeKid "k2" is not one of the signing keys`},
		{name: "active kid is a PEM", keys: keysJSON(t, testKey{Kid: "k1", Pem: k1}), activeKid: k2, secret: k2, wantErr: "activeKid is invalid"},
		{name: "keys set, active kid empty", keys: keysJSON(t, testKey{Kid: "k1", Pem: k1}), activeKid: "", secret: k1, wantErr: "activeKid is empty"},
		{name: "active kid set, keys empty", keys: "", activeKid: "k1", wantErr: "signingKeys is empty"},
		{name: "empty list", keys: "[]", activeKid: "k1", wantErr: "signingKeys is empty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := apptoken.NewKeyring(tc.keys, tc.activeKid)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.NotContains(t, err.Error(), "BEGIN")
			if tc.secret != "" {
				assert.NotContains(t, err.Error(), pemBodySample(tc.secret))
			}
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	out, err := json.Marshal(v)
	require.NoError(t, err)
	return string(out)
}

// TestNewKeyring_ECParametersBlock covers the output of "openssl ecparam -name prime256v1 -genkey" without -noout.
func TestNewKeyring_ECParametersBlock(t *testing.T) {
	t.Parallel()

	prime256v1OID := []byte{0x06, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07}
	params := string(pem.EncodeToMemory(&pem.Block{Type: "EC PARAMETERS", Bytes: prime256v1OID}))
	key := newTestKey(t)
	k, err := apptoken.NewKeyring(keysJSON(t, testKey{Kid: "k1", Pem: params + ecPEM(t, key)}), "k1")
	require.NoError(t, err)
	assert.True(t, k.ActiveKey().Equal(key))
}

func TestNewKeyring_PKCS8(t *testing.T) {
	t.Parallel()

	key := newTestKey(t)
	k, err := apptoken.NewKeyring(keysJSON(t, testKey{Kid: "k1", Pem: pkcs8PEM(t, key)}), "k1")
	require.NoError(t, err)
	assert.True(t, k.ActiveKey().Equal(key))
}

func TestKeyring_JWKSShape(t *testing.T) {
	t.Parallel()

	key1 := newTestKey(t)
	key2 := newTestKey(t)
	k, err := apptoken.NewKeyring(keysJSON(t, testKey{Kid: "k1", Pem: ecPEM(t, key1)}, testKey{Kid: "k2", Pem: ecPEM(t, key2)}), "k2")
	require.NoError(t, err)

	assert.True(t, k.Configured())
	assert.Equal(t, "k2", k.ActiveKid())
	assert.True(t, k.ActiveKey().Equal(key2))

	var raw struct {
		Keys []map[string]any `json:"keys"`
	}
	require.NoError(t, json.Unmarshal(k.JWKS(), &raw))
	require.Len(t, raw.Keys, 2)
	for i, want := range []struct {
		kid string
		key *ecdsa.PrivateKey
	}{{"k1", key1}, {"k2", key2}} {
		entry := raw.Keys[i]
		fields := make([]string, 0, len(entry))
		for name := range entry {
			fields = append(fields, name)
		}
		assert.ElementsMatch(t, []string{"kty", "crv", "use", "alg", "kid", "x", "y"}, fields)
		assert.Equal(t, "EC", entry["kty"])
		assert.Equal(t, "P-256", entry["crv"])
		assert.Equal(t, "sig", entry["use"])
		assert.Equal(t, "ES256", entry["alg"])
		assert.Equal(t, want.kid, entry["kid"])
		x, y := coordinates(t, &want.key.PublicKey)
		assert.Equal(t, x, decodeCoordinate(t, entry["x"].(string)))
		assert.Equal(t, y, decodeCoordinate(t, entry["y"].(string)))
	}
}

func TestKeyring_JWKSPadsShortCoordinates(t *testing.T) {
	t.Parallel()

	key := newTestKey(t)
	for x, _ := coordinates(t, &key.PublicKey); x[0] != 0; x, _ = coordinates(t, &key.PublicKey) {
		key = newTestKey(t)
	}
	k, err := apptoken.NewKeyring(keysJSON(t, testKey{Kid: "k1", Pem: ecPEM(t, key)}), "k1")
	require.NoError(t, err)

	pub := publicKeysFromJWKS(t, k.JWKS())["k1"]
	require.NotNil(t, pub)
	assert.True(t, pub.Equal(&key.PublicKey))
}

func TestKeyring_JWKSVerifiesEveryKey(t *testing.T) {
	t.Parallel()

	keys := map[string]*ecdsa.PrivateKey{"k1": newTestKey(t), "k2": newTestKey(t)}
	k, err := apptoken.NewKeyring(keysJSON(t, testKey{Kid: "k1", Pem: ecPEM(t, keys["k1"])}, testKey{Kid: "k2", Pem: ecPEM(t, keys["k2"])}), "k2")
	require.NoError(t, err)

	public := publicKeysFromJWKS(t, k.JWKS())
	for kid, priv := range keys {
		token := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{"sub": "user"})
		token.Header["kid"] = kid
		signed, err := token.SignedString(priv)
		require.NoError(t, err)

		parsed, err := jwt.Parse(signed, func(token *jwt.Token) (any, error) {
			return public[token.Header["kid"].(string)], nil
		}, jwt.WithValidMethods([]string{"ES256"}))
		require.NoError(t, err, kid)
		assert.True(t, parsed.Valid, kid)
	}
}

// TestKeyring_JWKSAcceptedByStrictConsumer feeds the JWKS to the preview link
// key set, which accepts only the sandboxes-service key shape.
func TestKeyring_JWKSAcceptedByStrictConsumer(t *testing.T) {
	t.Parallel()

	keys := map[string]*ecdsa.PrivateKey{"k1": newTestKey(t), "k2": newTestKey(t)}
	k, err := apptoken.NewKeyring(keysJSON(t, testKey{Kid: "k1", Pem: ecPEM(t, keys["k1"])}, testKey{Kid: "k2", Pem: ecPEM(t, keys["k2"])}), "k1")
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(k.JWKS())
	}))
	t.Cleanup(srv.Close)

	set := jwks.New(jwks.Config{URL: srv.URL, RefreshInterval: time.Minute, MaxStaleness: time.Hour}, clockwork.NewFakeClock(), log.NewNopLogger())
	require.NoError(t, set.Refresh(t.Context()))
	for kid, priv := range keys {
		pub, err := set.Key(t.Context(), kid)
		require.NoError(t, err, kid)
		assert.True(t, pub.Equal(&priv.PublicKey), kid)
	}
}
