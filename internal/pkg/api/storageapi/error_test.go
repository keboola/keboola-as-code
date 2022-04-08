package storageapi

import (
	"net/http"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/assert"
)

func TestErrorMsg1(t *testing.T) {
	t.Parallel()
	e := &Error{Message: "msg", response: newResponseWithStatusCode(404)}
	assert.Equal(t, `msg, method: "GET", url: "https://example.com", httpCode: "404"`, e.Error())
}

func TestErrorMsg2(t *testing.T) {
	t.Parallel()
	e := &Error{Message: "msg", ErrCode: "errCode", ExceptionId: "exceptionId", response: newResponseWithStatusCode(404)}
	assert.Equal(t, `msg, method: "GET", url: "https://example.com", httpCode: "404", errCode: "errCode", exceptionId: "exceptionId"`, e.Error())
}

func TestErrorHttpStatus(t *testing.T) {
	t.Parallel()
	e := &Error{}
	e.SetResponse(newResponseWithStatusCode(123))
	assert.Equal(t, 123, e.StatusCode())
}

func newResponseWithStatusCode(code int) *resty.Response {
	return &resty.Response{
		Request:     &resty.Request{Method: resty.MethodGet, URL: "https://example.com"},
		RawResponse: &http.Response{StatusCode: code},
	}
}
