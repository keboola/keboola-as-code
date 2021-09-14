package remote

import (
	"fmt"

	"github.com/go-resty/resty/v2"
)

type ErrorWithResponse interface {
	SetResponse(response *resty.Response)
	HttpStatus() int
	IsBadRequest() bool
	IsUnauthorized() bool
	IsForbidden() bool
	IsNotFound() bool
}

// Error represents Storage API error structure.
type Error struct {
	Message     string `json:"error"`
	ErrCode     string `json:"code"`
	ExceptionId string `json:"exceptionId"`
	response    *resty.Response
}

func (e *Error) Error() string {
	req := e.response.Request
	msg := fmt.Sprintf(`%s, method: "%s", url: "%s", httpCode: "%d"`, e.Message, req.Method, req.URL, e.HttpStatus())
	if len(e.ErrCode) > 0 {
		msg += fmt.Sprintf(`, errCode: "%s"`, e.ErrCode)
	}
	if len(e.ExceptionId) > 0 {
		msg += fmt.Sprintf(`, exceptionId: "%s"`, e.ExceptionId)
	}
	return msg
}

func (e *Error) SetResponse(response *resty.Response) {
	e.response = response
}

func (e *Error) HttpStatus() int {
	return e.response.StatusCode()
}

func (e *Error) IsBadRequest() bool {
	return e.HttpStatus() == 400
}

func (e *Error) IsUnauthorized() bool {
	return e.HttpStatus() == 401
}

func (e *Error) IsForbidden() bool {
	return e.HttpStatus() == 403
}

func (e *Error) IsNotFound() bool {
	return e.HttpStatus() == 404
}
