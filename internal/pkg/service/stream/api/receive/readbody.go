package receive

import (
	"io"
	"strings"

	"github.com/c2h5oh/datasize"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// ReadBody of the import endpoint, body length is limited.
func ReadBody(bodyReader io.ReadCloser) (str string, size int64, err error) {
	// Close reader at the end
	defer func() {
		if closeErr := bodyReader.Close(); closeErr != nil && err == nil {
			err = errors.Errorf("cannot close request body: %w", closeErr)
		}
	}()

	// Limit body size to LIMIT + 1 B.
	// If the reader fills the limit then the request is bigger than allowed.
	limit := store.MaxImportRequestSizeInBytes
	limitedReader := io.LimitReader(bodyReader, int64(limit+1))

	// Read request body
	var out strings.Builder
	size, err = io.Copy(&out, limitedReader)
	if err != nil {
		return "", 0, errors.Errorf("cannot read: %w", err)
	}

	// Check maximum size
	if datasize.ByteSize(size) > limit {
		return "", 0, serviceError.NewPayloadTooLargeError(errors.Wrapf(err, `payload too large, the maximum size is %s`, limit.String()))
	}

	return out.String(), size, nil
}
