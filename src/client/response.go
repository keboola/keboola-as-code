package client

import "github.com/go-resty/resty/v2"

type Response struct {
	request  *Request
	response *resty.Response
	err      error
}

func (r *Response) HasResponse() bool {
	return r.response != nil
}

func (r *Response) HasResult() bool {
	return !r.HasError() && r.response != nil && r.response.Result() != nil
}

func (r *Response) HasError() bool {
	return r.err != nil
}

func (r *Response) Request() *Request {
	return r.request
}

func (r *Response) RestyRequest() *resty.Request {
	return r.request.RestyRequest()
}

func (r *Response) RestyResponse() *resty.Response {
	return r.response
}

func (r *Response) Result() interface{} {
	if r.HasError() || r.response == nil {
		return nil
	}
	return r.response.Result()
}

func (r *Response) SetResult(result interface{}) *Response {
	r.RestyRequest().Result = result
	return r
}

func (r *Response) Error() error {
	return r.err
}

func (r *Response) SetError(err error) *Response {
	r.err = err
	return r
}

func (r *Response) Url() string {
	return r.response.Request.URL
}

func NewResponse(request *Request, response *resty.Response, err error) *Response {
	return &Response{request, response, err}
}
