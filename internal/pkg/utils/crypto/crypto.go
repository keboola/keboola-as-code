package crypto

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
)

// GenerateRSAKeyPairPEM generates a 2048-bit RSA key pair suitable for Snowflake key-pair authentication.
// The private key is encoded as PKCS#8 PEM without encryption, and the public key is PKIX PEM.
// Returns the private key PEM, public key PEM, and any error that occurred during generation.
func GenerateRSAKeyPairPEM() (privateKeyPEM string, publicKeyPEM string, err error) {
	// Generate private key
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", err
	}

	// Marshal private key to PKCS#8
	pkcs8Bytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return "", "", err
	}
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8Bytes})

	// Marshal public key to PKIX
	pubBytes, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		return "", "", err
	}
	pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubBytes})

	return string(privPEM), string(pubPEM), nil
}
