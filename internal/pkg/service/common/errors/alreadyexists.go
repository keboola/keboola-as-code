package errors

import (
	"fmt"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ResourceAlreadyExistsError struct {
	what string
	key  string
	in   string
}

func NewResourceAlreadyExistsError(what, key, in string) ResourceAlreadyExistsError {
	return ResourceAlreadyExistsError{what: what, key: key, in: in}
}

func (e ResourceAlreadyExistsError) ErrorName() string {
	return fmt.Sprintf("%sAlreadyExists", e.what)
}

func (e ResourceAlreadyExistsError) StatusCode() int {
	return http.StatusConflict
}

func (e ResourceAlreadyExistsError) Error() string {
	return fmt.Sprintf(`%s "%s" already exists in the %s`, e.what, e.key, e.in)
}

func (e ResourceAlreadyExistsError) ErrorUserMessage() string {
	return errors.Format(e, errors.FormatAsSentences())
}
