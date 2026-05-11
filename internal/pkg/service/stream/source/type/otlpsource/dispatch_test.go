package otlpsource

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// statusErr satisfies svcerrors.WithStatusCode and is enough to drive the
// dispatch result aggregation without spinning up a real dispatcher.
type statusErr struct {
	msg  string
	code int
}

func (e *statusErr) Error() string  { return e.msg }
func (e *statusErr) StatusCode() int { return e.code }

func TestStatusCodeFromError_FallbackTo500(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 500, statusCodeFromError(errors.New("plain")))
}

func TestStatusCodeFromError_UsesWithStatusCode(t *testing.T) {
	t.Parallel()

	err := &statusErr{msg: "x", code: http.StatusServiceUnavailable}
	assert.Equal(t, http.StatusServiceUnavailable, statusCodeFromError(err))
}

func TestRecordOutcome_NilIsNoop(t *testing.T) {
	t.Parallel()

	r := DispatchResult{Total: 5}
	recordOutcome(&r, nil)
	assert.Equal(t, 0, r.Rejected)
	assert.Equal(t, 0, r.WorstStatusCode)
	assert.NoError(t, r.FirstError)
}

func TestRecordOutcome_AggregatesErrors(t *testing.T) {
	t.Parallel()

	r := DispatchResult{Total: 3}
	recordOutcome(&r, &statusErr{msg: "bad request", code: 400})
	recordOutcome(&r, &statusErr{msg: "unavailable", code: 503})
	recordOutcome(&r, errors.New("unknown"))

	assert.Equal(t, 3, r.Rejected)
	assert.Equal(t, 503, r.WorstStatusCode, "worst should be the highest 5xx seen")
	require.Error(t, r.FirstError)
	assert.Contains(t, r.FirstError.Error(), "bad request", "FirstError preserved across subsequent failures")
}
