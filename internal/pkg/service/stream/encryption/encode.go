package encryption

import (
	"bytes"
	"encoding/gob"
	"sort"

	"github.com/pkg/errors"
)

// Encode converts a metadata map to byte slice.
func Encode(data map[string]string) ([]byte, error) {
	type pair struct {
		Key   string
		Value string
	}

	slice := make([]pair, len(data))
	i := 0
	for k, v := range data {
		slice[i] = pair{
			Key:   k,
			Value: v,
		}
		i += 1
	}

	// Sort the slice to make the result deterministic
	sort.Slice(slice, func(i, j int) bool { return slice[i].Key < slice[j].Key })

	var buffer bytes.Buffer

	err := gob.NewEncoder(&buffer).Encode(slice)
	if err != nil {
		return nil, errors.Wrapf(err, "gob encoder failed: %s", err.Error())
	}

	return buffer.Bytes(), nil
}
