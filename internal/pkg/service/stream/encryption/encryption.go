package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"

	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ProviderNone    = Provider("none")
	ProviderTesting = Provider("testing")
	ProviderNative  = Provider("native")
	ProviderGCP     = Provider("gcp")
	ProviderAWS     = Provider("aws")
	ProviderAzure   = Provider("azure")
)

type Provider string

func NewEncryptor(ctx context.Context, config Config) (cloudencrypt.Encryptor, error) {
	var encryptor cloudencrypt.Encryptor
	var err error

	switch config.Provider {
	case ProviderNone:
		return nil, nil
	case ProviderTesting:
		encryptor, err = NewTestingEncryptor(config.Native.SecretKey)
		if err != nil {
			return nil, err
		}

		return encryptor, nil
	case ProviderNative:
		encryptor, err = cloudencrypt.NewNativeEncryptor(config.Native.SecretKey)
		if err != nil {
			return nil, err
		}

		return encryptor, nil
	case ProviderGCP:
		encryptor, err = cloudencrypt.NewGCPEncryptor(ctx, config.GCP.KMSKeyID)
		if err != nil {
			return nil, err
		}
	case ProviderAWS:
		encryptor, err = cloudencrypt.NewAWSEncryptor(ctx, config.AWS.Region, config.AWS.KMSKeyID)
		if err != nil {
			return nil, err
		}
	case ProviderAzure:
		encryptor, err = cloudencrypt.NewAzureEncryptor(ctx, config.Azure.KeyVaultURL, config.Azure.KeyName)
		if err != nil {
			return nil, err
		}
	}

	encryptor, err = cloudencrypt.NewDualEncryptor(ctx, encryptor)
	if err != nil {
		return nil, err
	}

	return encryptor, nil
}

// testingEncryptor Implements Encryptor without using any cloud service using fixed random generator.
type TestingEncryptor struct {
	gcm cipher.AEAD
}

func NewTestingEncryptor(secretKey []byte) (*TestingEncryptor, error) {
	aesCipher, err := aes.NewCipher(secretKey)
	if err != nil {
		return nil, errors.Wrapf(err, "can't create aes cipher: %s", err.Error())
	}

	gcm, err := cipher.NewGCM(aesCipher)
	if err != nil {
		return nil, errors.Wrapf(err, "can't create gcm: %s", err.Error())
	}

	return &TestingEncryptor{
		gcm: gcm,
	}, nil
}

func (encryptor *TestingEncryptor) Encrypt(ctx context.Context, plaintext []byte, meta cloudencrypt.Metadata) ([]byte, error) {
	additionalData, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}

	serial := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	// Passing nonce as the first parameter prepends it to the actual encrypted value.
	return encryptor.gcm.Seal(serial, serial, plaintext, additionalData), nil
}

func (encryptor *TestingEncryptor) Decrypt(ctx context.Context, ciphertext []byte, meta cloudencrypt.Metadata) ([]byte, error) {
	additionalData, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}

	nonceSize := encryptor.gcm.NonceSize()
	// Split the ciphertext back to the nonce + actual ciphertext.
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]

	plaintext, err := encryptor.gcm.Open(nil, nonce, ciphertext, additionalData)
	if err != nil {
		return nil, errors.Wrapf(err, "gcm decryption failed: %s", err.Error())
	}

	return plaintext, nil
}

func (encryptor *TestingEncryptor) Close() error {
	return nil
}
