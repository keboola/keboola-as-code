package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-resty/resty/v2"
)

const (
	EventOnSuccess ResponseEventType = iota
	EventOnError
	EventOnResponse // always
)

// Sender of the request, client (for sync) or pool (for async).
type Sender interface {
	Send(r *Request)
	Request(request *Request) *Request
}

type ResponseEventType int

type ResponseCallback func(response *Response)

type ResponseListener struct {
	Type     ResponseEventType
	Callback ResponseCallback
}

type Request struct {
	*resty.Request
	*Response
	lock          *sync.Mutex
	id            int
	sent          bool
	done          bool
	url           string
	pathParams    map[string]string // for logs
	queryParams   map[string]string // for logs
	sender        Sender
	listeners     []*ResponseListener // callbacks invoked when request (and sub-requests from waitingFor) are completed
	waitingFor    []*Request          // all defined sub-requests must be completed, to continue listeners execution
	waitingForGrp *sync.WaitGroup     // blocks the execution of listeners until all sub-requests are completed
}

func NewRequest(id int, sender Sender, request *resty.Request) *Request {
	return &Request{
		Request:       request,
		lock:          &sync.Mutex{},
		id:            id,
		pathParams:    make(map[string]string),
		queryParams:   make(map[string]string),
		url:           request.URL,
		sender:        sender,
		waitingForGrp: &sync.WaitGroup{},
	}
}

func (r *Request) SetResult(result interface{}) *Request {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.Request.SetResult(result)
	return r
}

func (r *Request) SetHeader(header string, value string) *Request {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.Request.SetHeader(header, value)
	return r
}

func (r *Request) SetQueryParam(param, value string) *Request {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.Request.SetQueryParam(param, value)
	r.queryParams[param] = value
	return r
}

func (r *Request) SetPathParam(param, value string) *Request {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.Request.SetPathParam(param, value)
	r.pathParams[param] = value
	return r
}

func (r *Request) SetFormBody(body map[string]string) *Request {
	r.lock.Lock()
	defer r.lock.Unlock()
	// Storage API use "form-urlencoded", but it can be simply switched to JSON in the future
	r.Request.SetHeader("Content-Type", "application/x-www-form-urlencoded")
	r.Request.SetFormData(body)
	return r
}

func (r *Request) SetJsonBody(body map[string]string) *Request {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.Request.SetHeader("Content-Type", "application/json")
	r.Request.SetBody(body) // nolint

	return r
}

func (r *Request) Send() *Request {
	if r.sent {
		panic(fmt.Errorf("the request has already been sent"))
	}
	r.sender.Send(r)
	return r
}

func (r *Request) MarkSent() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.sent = true
}

func (r *Request) IsSent() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.sent
}

func (r *Request) MarkDone(response *Response) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.Response = response
	r.done = true
}

func (r *Request) IsDone() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.done
}

func (r *Request) Id() int {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.id
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
	r.lock.Lock()
	defer r.lock.Unlock()
	// Store pathParams and queryParams to context for logs
	ctx = context.WithValue(ctx, contextKey("pathParams"), r.pathParams)
	ctx = context.WithValue(ctx, contextKey("queryParams"), r.queryParams)
	r.Request.SetContext(ctx)
	return r
}

// WaitFor ensures that all main-request listeners which have not yet been executed
// will be deferred until sub-request will be done.
// All sub-request listeners defined before calling WaitFor
// will be executed before main-request listeners.
// See TestWaitForSubRequest test.
func (r *Request) WaitFor(subRequest *Request) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Error from the targetRequest will be set to the parent request, if it is already processed.
	inheritError := r.done

	// Store sub-request
	r.waitingFor = append(r.waitingFor, subRequest)

	// Continue execution of the listeners, when the sub-request completes
	r.waitingForGrp.Add(1)
	subRequest.OnResponse(func(response *Response) {
		defer r.waitingForGrp.Done()

		// Copy error from sub-request to parent request
		if inheritError && subRequest.HasError() && !r.HasError() {
			r.SetErr(subRequest.Err())
		}
	})
}

// nextListener which has not yet been invoked.
func (r *Request) nextListener() *ResponseListener {
	// Wait until all sub-requests done
	r.waitingForGrp.Wait()

	// Atomic access to the listeners slice
	r.lock.Lock()
	defer r.lock.Unlock()

	// No more listeners to invoke
	if len(r.listeners) == 0 {
		return nil
	}

	// Are all sub-requests done?
	for _, subRequest := range r.waitingFor {
		if !subRequest.IsDone() {
			panic(fmt.Errorf(`all sub-request should be done`))
		}
	}

	// Remove listener from slice
	listener := r.listeners[0]
	r.listeners = r.listeners[1:]
	return listener
}

// invokeListeners if all "waitingFor" requests are done.
func (r *Request) invokeListeners() {
	for {
		// Invoke next listener if present
		listener := r.nextListener()
		if listener != nil {
			listener.Invoke(r.Response)
		} else {
			// all done
			return
		}

		// Stop if context is cancelled
		select {
		case <-r.Context().Done():
			// request has been cancelled
			return
		default:
			// continue
		}
	}
}

func (r *Request) addListener(t ResponseEventType, callback ResponseCallback) *Request {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.sent {
		panic("listener cannot be added, request is already sent")
	}

	listener := &ResponseListener{t, callback}
	r.listeners = append(r.listeners, listener)
	return r
}

func (l *ResponseListener) Invoke(response *Response) {
	if l.Type == EventOnSuccess && response.err != nil {
		// Invoke only if no error present
		return
	}
	if l.Type == EventOnError && response.err == nil {
		// Invoke only if error present
		return
	}

	l.Callback(response)
}
