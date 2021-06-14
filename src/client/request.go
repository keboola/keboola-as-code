package client

import (
	"context"
	"github.com/go-resty/resty/v2"
)

type DecoratorFunc func(response *resty.Response, err error) (*resty.Response, error)

type Request struct {
	request   *resty.Request
	url       string
	decorator DecoratorFunc
}

func (r *Request) Url() string {
	return r.url
}
func (r *Request) Method() string {
	return r.request.Method
}

func (r *Request) Request() *resty.Request {
	return r.request
}

func (r *Request) SetContext(ctx context.Context) *Request {
	r.request.SetContext(ctx)
	return r
}

func (r *Request) Decorate(response *resty.Response, err error) (*resty.Response, error) {
	if r.decorator != nil {
		response, err = r.decorator(response, err)
	}
	return response, err
}

func NewRequest(request *resty.Request) *Request {
	return NewRequestWithDecorator(request, nil)
}

func NewRequestWithDecorator(request *resty.Request, decorator DecoratorFunc) *Request {
	return &Request{request, request.URL, decorator}
}
