package apptoken

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"

	sdk "github.com/keboola/keboola-sdk-go/v2/pkg/apptoken"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const minRefreshSecretBytes = 32

// RefreshKeyring holds the HS256 keys of refresh tokens. They are never published in the JWKS.
type RefreshKeyring struct {
	activeKid string
	keys      map[string][]byte
}

type refreshKey struct {
	Kid    string `json:"kid"`
	Secret string `json:"secret"`
}

func NewRefreshKeyring(keysJSON, activeKid string) (*RefreshKeyring, error) {
	if keysJSON == "" && activeKid == "" {
		return &RefreshKeyring{}, nil
	}

	entries, err := parseRefreshKeys(keysJSON)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, errors.New("app user token: refreshKeys is empty but refreshActiveKid is set")
	}
	if activeKid == "" {
		return nil, errors.New("app user token: refreshActiveKid is empty but refreshKeys are set")
	}

	keys := make(map[string][]byte, len(entries))
	for i, entry := range entries {
		secret, err := parseRefreshKey(i, entry, keys)
		if err != nil {
			return nil, err
		}
		keys[entry.Kid] = secret
	}

	if _, ok := keys[activeKid]; !ok {
		return nil, errors.Errorf(`app user token: refreshActiveKid "%s" is not one of the refresh keys`, activeKid)
	}
	return &RefreshKeyring{activeKid: activeKid, keys: keys}, nil
}

func parseRefreshKeys(keysJSON string) ([]refreshKey, error) {
	if keysJSON == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(keysJSON)))
	decoder.DisallowUnknownFields()
	var entries []refreshKey
	// The decoder error is dropped: it can quote a fragment of the value.
	err := decoder.Decode(&entries)
	if err == nil {
		_, err = decoder.Token()
	}
	if !errors.Is(err, io.EOF) {
		return nil, errors.New(`app user token: refreshKeys is not a JSON list of {"kid": "...", "secret": "..."} objects`)
	}
	return entries, nil
}

func parseRefreshKey(i int, entry refreshKey, seen map[string][]byte) ([]byte, error) {
	if !sdk.ValidKID(entry.Kid) {
		return nil, errors.Errorf(`app user token: refreshKeys[%d]: invalid kid "%s", it must match %s`, i, entry.Kid, sdk.KIDPattern)
	}
	if _, ok := seen[entry.Kid]; ok {
		return nil, errors.Errorf(`app user token: refreshKeys[%d]: duplicate kid "%s"`, i, entry.Kid)
	}
	secret, err := base64.StdEncoding.Strict().DecodeString(entry.Secret)
	if err != nil || len(secret) < minRefreshSecretBytes {
		return nil, errors.Errorf(`app user token: refreshKeys[%d] (kid "%s"): secret is not standard base64 of at least %d bytes`, i, entry.Kid, minRefreshSecretBytes)
	}
	return secret, nil
}

func (k *RefreshKeyring) Configured() bool {
	return k.activeKid != ""
}

func (k *RefreshKeyring) ActiveKid() string {
	return k.activeKid
}

func (k *RefreshKeyring) activeKey() []byte {
	return k.keys[k.activeKid]
}

func (k *RefreshKeyring) key(kid string) ([]byte, bool) {
	key, ok := k.keys[kid]
	return key, ok
}
