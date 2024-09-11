package csvfmt

import (
	"strconv"
	"unsafe"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func Format(v any) ([]byte, error) {
	// Inspired by cast.ToStringE(), but without slow reflection
	switch s := v.(type) {
	case []byte:
		return s, nil
	case string:
		return []byte(s), nil
	case bool:
		return strToBytes(strconv.FormatBool(s)), nil
	case float64:
		return strToBytes(strconv.FormatFloat(s, 'f', -1, 64)), nil
	case float32:
		return strToBytes(strconv.FormatFloat(float64(s), 'f', -1, 32)), nil
	case int:
		return strToBytes(strconv.FormatInt(int64(s), 10)), nil
	case int64:
		return strToBytes(strconv.FormatInt(s, 10)), nil
	case int32:
		return strToBytes(strconv.FormatInt(int64(s), 10)), nil
	case int16:
		return strToBytes(strconv.FormatInt(int64(s), 10)), nil
	case int8:
		return strToBytes(strconv.FormatInt(int64(s), 10)), nil
	case uint:
		return strToBytes(strconv.FormatUint(uint64(s), 10)), nil
	case uint64:
		return strToBytes(strconv.FormatUint(s, 10)), nil
	case uint32:
		return strToBytes(strconv.FormatUint(uint64(s), 10)), nil
	case uint16:
		return strToBytes(strconv.FormatUint(uint64(s), 10)), nil
	case uint8:
		return strToBytes(strconv.FormatUint(uint64(s), 10)), nil
	case nil:
		return nil, nil
	default:
		return nil, errors.Errorf("unable to cast %#v of type %T to string", v, v)
	}
}

func strToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
