package key

import (
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type (
	ProjectID keboola.ProjectID
)

func (v ProjectID) String() string {
	if v == 0 {
		panic(errors.New("projectID cannot be empty"))
	}
	return fmt.Sprintf("%08d", v)
}
