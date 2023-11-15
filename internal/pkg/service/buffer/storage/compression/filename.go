package compression

import "github.com/keboola/keboola-as-code/internal/pkg/utils/errors"

func Filename(base string, t Type) (string, error) {
	switch t {
	case TypeNone:
		return base, nil
	case TypeGZIP:
		return base + ".gz", nil
	case TypeZSTD:
		return base + ".zstd", nil
	default:
		return "", errors.Errorf(`unexpected compression type "%s"`, t)
	}
}
