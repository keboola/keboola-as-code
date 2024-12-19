package encryption

import (
	"context"

	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"
	"github.com/pkg/errors"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// GCPEncryptor Implements Encryptor using Google Cloud's Key Management Service.
type GCPEncryptor struct {
	client *kms.KeyManagementClient
	keyID  string
	logger log.Logger
}

func NewGCPEncryptor(ctx context.Context, keyID string, logger log.Logger) (*GCPEncryptor, error) {
	client, err := kms.NewKeyManagementClient(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "can't create gpc kms client: %s", err.Error())
	}

	return &GCPEncryptor{
		client: client,
		keyID:  keyID,
		logger: logger,
	}, nil
}

func (encryptor *GCPEncryptor) Encrypt(ctx context.Context, plaintext []byte, metadata cloudencrypt.Metadata) ([]byte, error) {
	additionalData, err := Encode(metadata)
	if err != nil {
		return nil, err
	}

	encryptor.logger.Infof(ctx, "encryption key: %s", encryptor.keyID)
	encryptor.logger.Infof(ctx, "encryption metadata: %s", string(additionalData))
	encryptor.logger.Infof(ctx, "encryption plaintext: %s", string(plaintext))

	request := &kmspb.EncryptRequest{
		Name:                        encryptor.keyID,
		Plaintext:                   plaintext,
		AdditionalAuthenticatedData: additionalData,
	}

	response, err := encryptor.client.Encrypt(ctx, request)
	if err != nil {
		return nil, errors.Wrapf(err, "gcp encryption failed: %s", err.Error())
	}

	encryptor.logger.Infof(ctx, "encryption ciphertext: %s", string(response.GetCiphertext()))

	return response.GetCiphertext(), nil
}

func (encryptor *GCPEncryptor) Decrypt(ctx context.Context, ciphertext []byte, metadata cloudencrypt.Metadata) ([]byte, error) {
	additionalData, err := Encode(metadata)
	if err != nil {
		return nil, err
	}

	encryptor.logger.Infof(ctx, "decryption key: %s", encryptor.keyID)
	encryptor.logger.Infof(ctx, "decryption metadata: %s", string(additionalData))
	encryptor.logger.Infof(ctx, "decryption ciphertext: %s", string(ciphertext))

	request := &kmspb.DecryptRequest{
		Name:                        encryptor.keyID,
		Ciphertext:                  ciphertext,
		AdditionalAuthenticatedData: additionalData,
	}

	response, err := encryptor.client.Decrypt(ctx, request)
	if err != nil {
		return nil, errors.Wrapf(err, "gcp decryption failed: %s", err.Error())
	}

	encryptor.logger.Infof(ctx, "decryption plaintext: %s", string(response.GetPlaintext()))

	return response.GetPlaintext(), nil
}

func (encryptor *GCPEncryptor) Close() error {
	err := encryptor.client.Close()
	if err != nil {
		return errors.Wrapf(err, "can't close gcp client: %s", err.Error())
	}

	return nil
}
