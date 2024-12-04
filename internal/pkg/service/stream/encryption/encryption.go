package encryption

import (
	"context"

	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"
)

const (
	ProviderNone   = Provider("none")
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
	case ProviderNone:
		return nil, nil
	case ProviderNative:
		encryptor, err = cloudencrypt.NewNativeEncryptor([]byte(config.Native.SecretKey))
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
