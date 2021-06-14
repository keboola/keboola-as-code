package client

import (
	"context"
	"fmt"
	"github.com/go-resty/resty/v2"
)

type DecoratorFunc func(response *resty.Response, err error) (*resty.Response, error)
type ResponseEventType int
type ResponseCallback func(response *Response) *Response
type ResponseListener struct {
	Type     ResponseEventType
	Callback ResponseCallback
}
type Sender interface {
	Send(r *Request)
}

const (
	EventOnSuccess ResponseEventType = iota
	EventOnError
	EventOnResponse // always
)

type Request struct {
	id        int
	request   *resty.Request
	response  *Response
	url       string
	sender    Sender
	sent      bool
	listeners []*ResponseListener
}

func (r *Request) SetResult(result interface{}) *Request {
	r.request.SetResult(result)
	return r
}

func (r *Request) SetHeader(header string, value string) *Request {
	r.request.SetHeader(header, value)
	return r
}

func (r *Request) SetQueryParam(param, value string) *Request {
	r.request.SetQueryParam(param, value)
	return r
}

func (r *Request) SetMultipartFormData(data map[string]string) *Request {
	r.request.SetMultipartFormData(data)
	return r
}

func (r *Request) Send() *Request {
	r.sender.Send(r)
	return r
}

func (r *Request) Id() int {
	return r.id
}

func (r *Request) Url() string {
	return r.url
}

func (r *Request) Method() string {
	return r.request.Method
}

func (r *Request) RestyRequest() *resty.Request {
	return r.request
}

func (r *Request) Response() *Response {
	if r.response == nil {
		panic(fmt.Errorf("response is not set"))
	}
	return r.response
}

func (r *Request) OnResponse(callback ResponseCallback) *Request {
	return r.addListener(EventOnResponse, callback)
}

func (r *Request) OnSuccess(callback ResponseCallback) *Request {
	return r.addListener(EventOnSuccess, callback)
}

func (r *Request) OnError(callback ResponseCallback) *Request {
	return r.addListener(EventOnError, callback)
}

func (r *Request) SetContext(ctx context.Context) *Request {
	r.request.SetContext(ctx)
	return r
}

func (r *Request) invokeListeners() {
	for _, listener := range r.listeners {
		r.response = listener.Invoke(r.response)
	}
}

func (r *Request) addListener(t ResponseEventType, callback ResponseCallback) *Request {
	if r.sent {
		panic("listener cannot be added, request is already sent")
	}

	listener := &ResponseListener{t, callback}
	r.listeners = append(r.listeners, listener)
	return r
}

func (l *ResponseListener) Invoke(response *Response) *Response {
	if l.Type == EventOnSuccess && response.err != nil {
		// Invoke only if no error present
		return response
	}
	if l.Type == EventOnError && response.err == nil {
		// Invoke only if error present
		return response
	}

	return l.Callback(response)
}

func NewRequest(id int, sender Sender, request *resty.Request) *Request {
	return &Request{id: id, request: request, url: request.URL, sender: sender}
}
