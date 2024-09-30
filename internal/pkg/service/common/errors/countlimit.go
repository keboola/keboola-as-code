package errors

import (
	"fmt"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type CountLimitReachedError struct {
	what string
	max  int
	in   string
}

func NewCountLimitReachedError(what string, maximum int, in string) CountLimitReachedError {
	return CountLimitReachedError{what: what, max: maximum, in: in}
}

func (e CountLimitReachedError) ErrorName() string {
	return fmt.Sprintf("%sCountLimitReached", e.what)
}

func (e CountLimitReachedError) StatusCode() int {
	return http.StatusConflict
}

func (e CountLimitReachedError) Error() string {
	return fmt.Sprintf("%s count limit reached in the %s, the maximum is %d", e.what, e.in, e.max)
}

func (e CountLimitReachedError) ErrorUserMessage() string {
	return errors.Format(e, errors.FormatAsSentences())
}
