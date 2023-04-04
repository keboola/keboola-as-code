package key

import "github.com/keboola/keboola-as-code/internal/pkg/utils/errors"

type (
	ID string
)

func (v ID) String() string {
	if v == "" {
		panic(errors.New("task ID cannot be empty"))
	}
	return string(v)
}
