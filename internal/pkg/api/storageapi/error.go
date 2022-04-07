package storageapi

import (
	"fmt"

	"github.com/go-resty/resty/v2"
)

type ErrorWithResponse interface {
	SetResponse(response *resty.Response)
	StatusCode() int
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
	msg := fmt.Sprintf(`%s, method: "%s", url: "%s", httpCode: "%d"`, e.Message, req.Method, req.URL, e.StatusCode())
	if len(e.ErrCode) > 0 {
		msg += fmt.Sprintf(`, errCode: "%s"`, e.ErrCode)
	}
	if len(e.ExceptionId) > 0 {
		msg += fmt.Sprintf(`, exceptionId: "%s"`, e.ExceptionId)
	}
	return msg
}

func (e *Error) ErrorName() string {
	return e.ErrCode
}

func (e *Error) ErrorUserMessage() string {
	return e.Message
}

func (e *Error) ErrorExceptionId() string {
	return e.ExceptionId
}

func (e *Error) SetResponse(response *resty.Response) {
	e.response = response
}

func (e *Error) StatusCode() int {
	if e.response == nil {
		return 500
	}
	return e.response.StatusCode()
}
