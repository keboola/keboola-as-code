package apptoken_test

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
)

func refreshSecret(t *testing.T, size int) string {
	t.Helper()
	b := make([]byte, size)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(b)
}

func refreshKeysJSON(t *testing.T, kids ...string) string {
	t.Helper()
	out, _ := refreshKeysWithSecrets(t, kids...)
	return out
}

func refreshKeysWithSecrets(t *testing.T, kids ...string) (string, map[string][]byte) {
	t.Helper()
	type entry struct {
		Kid    string `json:"kid"`
		Secret string `json:"secret"`
	}
	entries := make([]entry, 0, len(kids))
	secrets := make(map[string][]byte, len(kids))
	for _, kid := range kids {
		secret := refreshSecret(t, 32)
		raw, err := base64.StdEncoding.DecodeString(secret)
		require.NoError(t, err)
		secrets[kid] = raw
		entries = append(entries, entry{Kid: kid, Secret: secret})
	}
	out, err := json.Marshal(entries)
	require.NoError(t, err)
	return string(out), secrets
}

func TestNewRefreshKeyring_Disabled(t *testing.T) {
	t.Parallel()

	keyring, err := apptoken.NewRefreshKeyring("", "")
	require.NoError(t, err)
	assert.False(t, keyring.Configured())
}

func TestNewRefreshKeyring_Valid(t *testing.T) {
	t.Parallel()

	keyring, err := apptoken.NewRefreshKeyring(refreshKeysJSON(t, "r1", "r2"), "r2")
	require.NoError(t, err)
	assert.True(t, keyring.Configured())
	assert.Equal(t, "r2", keyring.ActiveKid())
}

func TestNewRefreshKeyring_Invalid(t *testing.T) {
	t.Parallel()

	short := refreshSecret(t, 31)
	cases := []struct {
		name    string
		keys    string
		active  string
		wantErr string
	}{
		{name: "not json", keys: `{`, active: "r1", wantErr: `app user token: refreshKeys is not a JSON list of {"kid": "...", "secret": "..."} objects`},
		{name: "unknown field", keys: `[{"kid":"r1","secret":"x","pem":"y"}]`, active: "r1", wantErr: `refreshKeys is not a JSON list`},
		{name: "trailing data", keys: `[] []`, active: "r1", wantErr: `refreshKeys is not a JSON list`},
		{name: "empty list", keys: `[]`, active: "r1", wantErr: "app user token: refreshKeys is empty but refreshActiveKid is set"},
		{name: "no active kid", keys: refreshKeysJSON(t, "r1"), active: "", wantErr: "app user token: refreshActiveKid is empty but refreshKeys are set"},
		{name: "invalid kid", keys: `[{"kid":"a b","secret":"` + refreshSecret(t, 32) + `"}]`, active: "a b", wantErr: `app user token: refreshKeys[0]: invalid kid "a b", it must match ^[A-Za-z0-9._-]{1,64}$`},
		{name: "duplicate kid", keys: `[{"kid":"r1","secret":"` + refreshSecret(t, 32) + `"},{"kid":"r1","secret":"` + refreshSecret(t, 32) + `"}]`, active: "r1", wantErr: `app user token: refreshKeys[1]: duplicate kid "r1"`},
		{name: "empty secret", keys: `[{"kid":"r1","secret":""}]`, active: "r1", wantErr: `app user token: refreshKeys[0] (kid "r1"): secret is not standard base64 of at least 32 bytes`},
		{name: "short secret", keys: `[{"kid":"r1","secret":"` + short + `"}]`, active: "r1", wantErr: `app user token: refreshKeys[0] (kid "r1"): secret is not standard base64 of at least 32 bytes`},
		{name: "url base64 secret", keys: `[{"kid":"r1","secret":"` + strings.Repeat("-_", 22) + `"}]`, active: "r1", wantErr: `app user token: refreshKeys[0] (kid "r1"): secret is not standard base64`},
		{name: "unpadded secret", keys: `[{"kid":"r1","secret":"` + strings.TrimRight(refreshSecret(t, 32), "=") + `"}]`, active: "r1", wantErr: `app user token: refreshKeys[0] (kid "r1"): secret is not standard base64`},
		{name: "active kid unknown", keys: refreshKeysJSON(t, "r1"), active: "r2", wantErr: `app user token: refreshActiveKid "r2" is not one of the refresh keys`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := apptoken.NewRefreshKeyring(tc.keys, tc.active)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestNewRefreshKeyring_ErrorNeverContainsSecret(t *testing.T) {
	t.Parallel()

	for _, secret := range []string{refreshSecret(t, 31), "not-base64-but-secret-value-0123456789", `"quoted"`} {
		keys := `[{"kid":"r1","secret":` + mustJSON(t, secret) + `}]`
		_, err := apptoken.NewRefreshKeyring(keys, "r1")
		require.Error(t, err)
		assert.NotContains(t, err.Error(), secret)
	}

	_, err := apptoken.NewRefreshKeyring(`[{"kid":"r1","secret":"top-secret-value",}]`, "r1")
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "top-secret-value")
}
