package encryption

import (
	"context"

	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"
)

const (
	ProviderNone  = Provider("none")
	ProviderAES   = Provider("aes")
	ProviderGCP   = Provider("gcp")
	ProviderAWS   = Provider("aws")
	ProviderAzure = Provider("azure")
)

type Provider string

func NewEncryptor(ctx context.Context, config Config) (cloudencrypt.Encryptor, error) {
	var encryptor cloudencrypt.Encryptor
	var err error

	switch config.Provider {
	case ProviderNone:
		return nil, nil
	case ProviderAES:
		if config.AES.NonceGenerator != nil {
			encryptor, err = cloudencrypt.NewAESEncryptorWithNonceGenerator(config.AES.SecretKey, config.AES.NonceGenerator)
		} else {
			encryptor, err = cloudencrypt.NewAESEncryptor(config.AES.SecretKey)
		}

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

	prefix := string(config.Provider) + "::"

	if config.Provider != ProviderAES {
		prefix += string(ProviderAES) + "::"
		encryptor, err = cloudencrypt.NewAESWrapEncryptor(ctx, encryptor)
		if err != nil {
			return nil, err
		}
	}

	encryptor, err = cloudencrypt.NewBase64Encryptor(encryptor)
	if err != nil {
		return nil, err
	}

	encryptor, err = cloudencrypt.NewPrefixEncryptor(encryptor, []byte(prefix))
	if err != nil {
		return nil, err
	}

	return encryptor, nil
}
