package client

import (
	"sync"

	"github.com/go-resty/resty/v2"
)

type Response struct {
	*Request
	*resty.Response
	lock *sync.Mutex
	err  error // in resty has error type interface{}, but we need error type
}

func (r *Response) HasResponse() bool {
	return r.Response != nil
}

func (r *Response) HasResult() bool {
	return r.Response.Result() != nil
}

func (r *Response) HasError() bool {
	return r.err != nil
}

func (r *Response) SetResult(result interface{}) *Response {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.Request.Result = result
	r.Request.Error = nil
	return r
}

func (r *Response) Err() error {
	return r.err
}

func (r *Response) SetErr(err error) *Response {
	// Sub-request can run in parallel and end with an error -> it can be set to parent request
	// ... so locking is required
	r.lock.Lock()
	defer r.lock.Unlock()
	r.err = err
	if err != nil {
		r.Request.Result = nil
	}
	return r
}

func (r *Response) Sender() Sender {
	return r.Request.sender
}

func NewResponse(request *Request, response *resty.Response, err error) *Response {
	r := &Response{Request: request, Response: response, lock: &sync.Mutex{}}
	r.SetErr(err)
	return r
}
