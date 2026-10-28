package apptoken

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"

	"github.com/go-jose/go-jose/v4"
	sdk "github.com/keboola/keboola-sdk-go/v2/pkg/apptoken"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ecParametersType = "EC PARAMETERS"
)

type Keyring struct {
	activeKid string
	activeKey *ecdsa.PrivateKey
	jwks      []byte
}

type signingKey struct {
	Kid string `json:"kid"`
	Pem string `json:"pem"`
}

func NewKeyring(keysJSON, activeKid string) (*Keyring, error) {
	if keysJSON == "" && activeKid == "" {
		return newKeyring("", nil, nil)
	}

	entries, err := parseSigningKeys(keysJSON)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, errors.New("app user token: signingKeys is empty but activeKid is set")
	}
	if activeKid == "" {
		return nil, errors.New("app user token: activeKid is empty but signingKeys are set")
	}

	kids := make([]string, 0, len(entries))
	keys := make(map[string]*ecdsa.PrivateKey, len(entries))
	for i, entry := range entries {
		key, err := parseSigningKey(i, entry, keys)
		if err != nil {
			return nil, err
		}
		kids = append(kids, entry.Kid)
		keys[entry.Kid] = key
	}

	active, ok := keys[activeKid]
	if !ok {
		return nil, errors.Errorf(`app user token: activeKid "%s" is not one of the signing keys`, activeKid)
	}

	public := make([]jose.JSONWebKey, 0, len(kids))
	for _, kid := range kids {
		public = append(public, jose.JSONWebKey{Key: &keys[kid].PublicKey, KeyID: kid, Algorithm: sdk.Algorithm, Use: sdk.KeyUse})
	}
	return newKeyring(activeKid, active, public)
}

func newKeyring(activeKid string, activeKey *ecdsa.PrivateKey, public []jose.JSONWebKey) (*Keyring, error) {
	if public == nil {
		public = []jose.JSONWebKey{}
	}
	jwks, err := json.Marshal(jose.JSONWebKeySet{Keys: public})
	if err != nil {
		return nil, errors.PrefixError(err, "app user token: cannot encode JWKS")
	}
	return &Keyring{activeKid: activeKid, activeKey: activeKey, jwks: jwks}, nil
}

func parseSigningKeys(keysJSON string) ([]signingKey, error) {
	if keysJSON == "" {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(keysJSON)))
	decoder.DisallowUnknownFields()
	var entries []signingKey
	// The decoder error is dropped: it can quote a fragment of the value.
	err := decoder.Decode(&entries)
	if err == nil {
		_, err = decoder.Token()
	}
	if !errors.Is(err, io.EOF) {
		return nil, errors.New(`app user token: signingKeys is not a JSON list of {"kid": "...", "pem": "..."} objects`)
	}
	return entries, nil
}

func parseSigningKey(i int, entry signingKey, seen map[string]*ecdsa.PrivateKey) (*ecdsa.PrivateKey, error) {
	if !sdk.ValidKID(entry.Kid) {
		return nil, errors.Errorf(`app user token: signingKeys[%d]: invalid kid "%s", it must match %s`, i, entry.Kid, sdk.KIDPattern)
	}
	if _, ok := seen[entry.Kid]; ok {
		return nil, errors.Errorf(`app user token: signingKeys[%d]: duplicate kid "%s"`, i, entry.Kid)
	}
	if entry.Pem == "" {
		return nil, errors.Errorf("app user token: signingKeys[%d]: pem is empty", i)
	}
	der, err := keyBlock(entry.Pem)
	if err != nil {
		return nil, errors.Errorf(`app user token: signingKeys[%d] (kid "%s"): %s`, i, entry.Kid, err.Error())
	}
	key, err := parsePrivateKey(der)
	if err != nil {
		return nil, errors.Errorf(`app user token: signingKeys[%d] (kid "%s"): not a parsable private key`, i, entry.Kid)
	}
	ecKey, ok := key.(*ecdsa.PrivateKey)
	if !ok || ecKey.Curve != elliptic.P256() {
		return nil, errors.Errorf(`app user token: signingKeys[%d] (kid "%s"): not an EC P-256 key`, i, entry.Kid)
	}
	return ecKey, nil
}

// keyBlock skips the EC PARAMETERS block that "openssl ecparam -genkey" writes without -noout.
func keyBlock(pemData string) ([]byte, error) {
	var keys []*pem.Block
	for block, rest := pem.Decode([]byte(pemData)); block != nil; block, rest = pem.Decode(rest) {
		if block.Type != ecParametersType {
			keys = append(keys, block)
		}
	}
	switch len(keys) {
	case 0:
		return nil, errors.New("no PEM block found")
	case 1:
		return keys[0].Bytes, nil
	default:
		return nil, errors.New("more than one PEM block")
	}
}

func parsePrivateKey(der []byte) (any, error) {
	if key, err := x509.ParseECPrivateKey(der); err == nil {
		return key, nil
	}
	return x509.ParsePKCS8PrivateKey(der)
}

func (k *Keyring) Configured() bool {
	return k.activeKey != nil
}

func (k *Keyring) ActiveKid() string {
	return k.activeKid
}

func (k *Keyring) ActiveKey() *ecdsa.PrivateKey {
	return k.activeKey
}

func (k *Keyring) JWKS() []byte {
	return k.jwks
}
