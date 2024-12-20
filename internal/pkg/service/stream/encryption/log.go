package encryption

import (
	"context"

	"github.com/keboola/go-cloud-encrypt/pkg/cloudencrypt"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// LoggedEncryptor wraps another Encryptor and adds logging.
type LoggedEncryptor struct {
	encryptor cloudencrypt.Encryptor
	logger    log.Logger
}

func NewLoggedEncryptor(ctx context.Context, encryptor cloudencrypt.Encryptor, logger log.Logger) (*LoggedEncryptor, error) {
	return &LoggedEncryptor{
		encryptor: encryptor,
		logger:    logger,
	}, nil
}

func (encryptor *LoggedEncryptor) Encrypt(ctx context.Context, plaintext []byte, metadata cloudencrypt.Metadata) ([]byte, error) {
	meta := ""
	for k, v := range metadata {
		meta += k + ": " + v + ", "
	}

	encryptor.logger.Infof(ctx, "encryption metadata: "+meta)

	if len(plaintext) == 0 {
		err := errors.New("text should not be empty")
		encryptor.logger.Infof(ctx, "encryption error: %s", err.Error())
		return nil, err
	}

	encryptedValue, err := encryptor.encryptor.Encrypt(ctx, plaintext, metadata)
	if err != nil {
		encryptor.logger.Infof(ctx, "encryption error: %s", err.Error())
		return nil, err
	}

	encryptor.logger.Info(ctx, "encryption success")

	return encryptedValue, nil
}

func (encryptor *LoggedEncryptor) Decrypt(ctx context.Context, ciphertext []byte, metadata cloudencrypt.Metadata) ([]byte, error) {
	meta := ""
	for k, v := range metadata {
		meta += k + ": " + v + ", "
	}

	encryptor.logger.Infof(ctx, "decryption metadata: "+meta)

	if len(ciphertext) == 0 {
		err := errors.New("text should not be empty")
		encryptor.logger.Infof(ctx, "decryption error: %s", err.Error())
		return nil, err
	}

	plaintext, err := encryptor.encryptor.Decrypt(ctx, ciphertext, metadata)
	if err != nil {
		encryptor.logger.Infof(ctx, "decryption error: %s", err.Error())
		return nil, err
	}

	encryptor.logger.Info(ctx, "decryption success")

	return plaintext, nil
}

func (encryptor *LoggedEncryptor) Close() error {
	return encryptor.encryptor.Close()
}
