package encryption

import (
	"context"
	"crypto/rand"
	"math/big"

	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"
)

const (
	ProviderNative = Provider("native")
	ProviderGCP    = Provider("gcp")
	ProviderAWS    = Provider("aws")
	ProviderAzure  = Provider("azure")
)

type Provider string

func NewEncryptor(ctx context.Context, config Config) (cloudencrypt.Encryptor, error) {
	var encryptor cloudencrypt.Encryptor
	var err error

	switch config.Provider {
	case ProviderNative:
		encryptor, err = cloudencrypt.NewNativeEncryptor([]byte(config.Native.SecretKey))
		if err != nil {
			return nil, err
		}
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

	if config.Provider != ProviderNative {
		encryptor, err = cloudencrypt.NewDualEncryptor(ctx, encryptor)
		if err != nil {
			return nil, err
		}
	}

	return encryptor, nil
}

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandomSecretKey() (string, error) {
	result := make([]byte, 32)
	charsetLength := big.NewInt(int64(len(charset)))

	for i := range result {
		randIndex, err := rand.Int(rand.Reader, charsetLength)
		if err != nil {
			return "", err
		}
		result[i] = charset[randIndex.Int64()]
	}

	return string(result), nil
}
