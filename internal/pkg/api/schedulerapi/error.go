package schedulerapi

import (
	"net/http"

	"github.com/go-resty/resty/v2"
)

// Error represents Scheduler API error structure.
type Error struct {
	Message     string `json:"error"`
	ErrCode     int    `json:"code"`
	ExceptionId string `json:"exceptionId"`
	response    *resty.Response
}

func (e *Error) ErrorName() string {
	return http.StatusText(e.ErrCode)
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
