package encryption

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/gob"

	"github.com/pkg/errors"
)

func Encode(data any) ([]byte, error) {
	var buffer bytes.Buffer

	// Base64 encode
	encoder := base64.NewEncoder(base64.StdEncoding, &buffer)

	// Gzip compress
	writer := gzip.NewWriter(encoder)

	// gob encode
	err := gob.NewEncoder(writer).Encode(data)
	if err != nil {
		return nil, errors.Wrapf(err, "gob encoder failed: %s", err.Error())
	}

	err = writer.Close()
	if err != nil {
		return nil, errors.Wrapf(err, "can't close gzip writer: %s", err.Error())
	}

	err = encoder.Close()
	if err != nil {
		return nil, errors.Wrapf(err, "base64 encoder failed: %s", err.Error())
	}

	return buffer.Bytes(), nil
}

func Decode[T any](data []byte) (decoded T, err error) {
	// Base64 decode
	decoder := base64.NewDecoder(base64.StdEncoding, bytes.NewReader(data))

	// Gzip uncompress
	reader, err := gzip.NewReader(decoder)
	if err != nil {
		return decoded, errors.Wrapf(err, "can't create gzip reader: %s", err.Error())
	}

	defer reader.Close()

	// gob decode
	err = gob.NewDecoder(reader).Decode(&decoded)
	if err != nil {
		return decoded, errors.Wrapf(err, "gob decoder failed: %s", err.Error())
	}

	return decoded, nil
}
