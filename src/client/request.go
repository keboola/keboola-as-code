package client

import (
	"context"
	"fmt"
	"github.com/go-resty/resty/v2"
	"sync"
)

const (
	EventOnSuccess ResponseEventType = iota
	EventOnError
	EventOnResponse // always
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
	Request(request *Request) *Request
}

type Request struct {
	lock       *sync.Mutex
	id         int
	sent       bool
	done       bool
	url        string
	request    *resty.Request
	response   *Response
	sender     Sender
	listeners  []*ResponseListener
	waitingFor []*Request
}

func NewRequest(id int, sender Sender, request *resty.Request) *Request {
	return &Request{lock: &sync.Mutex{}, id: id, request: request, url: request.URL, sender: sender}
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

func (r *Request) IsSent() bool {
	return r.sent
}

func (r *Request) IsDone() bool {
	return r.done
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

// WaitFor ensures that all remaining listeners will be deferred until subRequest done
// See TestWaitForSubRequest test
func (r *Request) WaitFor(subRequest *Request) {
	r.lock.Lock()
	r.waitingFor = append(r.waitingFor, subRequest)
	r.lock.Unlock()

	subRequest.OnResponse(func(response *Response) *Response {
		r.invokeListeners()
		return response
	})
}

func (r *Request) isWaiting() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, subRequest := range r.waitingFor {
		if !subRequest.done {
			return true
		}
	}
	return false
}

func (r *Request) nextListener() *ResponseListener {
	r.lock.Lock()
	defer r.lock.Unlock()

	// No more listeners to invoke
	if len(r.listeners) == 0 {
		return nil
	}

	// Remove listener from slice
	listener := r.listeners[0]
	r.listeners = r.listeners[1:]
	return listener
}

func (r *Request) invokeListeners() {
	for {
		if r.isWaiting() {
			// Invoke listeners later, when all subRequests will be done, see WaitFor method
			break
		}

		// Invoke next listener if present
		listener := r.nextListener()
		if listener != nil {
			r.response = listener.Invoke(r.response)
		} else {
			break
		}
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
