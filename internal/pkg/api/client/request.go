package client

import (
	"fmt"
	"net/http"
	"net/url"
)

// Sender is implementation of HTTP client, e.g. using go-resty/resty lib, or native net/http.
type Sender interface {
	// Send method sends defined request and returns response.
	// Type of the return value "result" must be the same as type of the argument "resultDef", otherwise panic will occur.
	//   In Go, this rule cannot be written using generic types yet, methods cannot have generic types.
	//   Send[R Result](request RequestReadOnly, resultDef R, errorDef error) (result R, error error)
	Send(request RequestReadOnly, resultDef interface{}, errorDef error) (rawResponse *http.Response, result interface{}, error error)
}

type Result = any

type NoResult struct{}

// Request is the definition of the HTTP request whose response should be mapped to type R.
type Request[R Result] interface {
	RequestReadOnly
	// SetMethod method sets the HTTP method.
	SetMethod(method string) Request[R]
	// SetUrl method sets the URL.
	SetUrl(url string) Request[R]
	// SetHeader method sets a single header field and its value.
	SetHeader(header string, value string) Request[R]
	// SetQueryParam method sets single parameter and its value.
	SetQueryParam(param, value string) Request[R]
	// SetQueryParams method sets multiple parameters and its values.
	SetQueryParams(params map[string]string) Request[R]
	// SetPathParam method sets single URL path key-value pair.
	SetPathParam(param, value string) Request[R]
	// SetPathParams method sets multiple URL path key-value pairs.
	SetPathParams(params map[string]string) Request[R]
	// SetFormBody method sets Form parameters, their values and Content-Type header to "application/x-www-form-urlencoded".
	SetFormBody(body map[string]string) Request[R]
	// SetJsonBody method sets request body to a JSON value and Content-Type header to "application/json".
	SetJsonBody(body map[string]string) Request[R]
	// SetErrorDef method is to register the request `Error` value for automatic mapping.
	SetErrorDef(err error) Request[R]
	// OnComplete method registers callback to be executed when the request is completed.
	OnComplete(func(request Request[R], result R, err error)) Request[R]
	// OnSuccess method registers callback to be executed when the request is completed and `code >= 200 and <= 299`.
	OnSuccess(func(request Request[R], result R)) Request[R]
	// OnError method registers callback to be executed when the request is completed and `code >= 400`.
	OnError(func(request Request[R], err error)) Request[R]
	// Send sends the request by the sender.
	Send(sender Sender) (Response[R], R, error)
}

// RequestReadOnly contains read only request data used by the Sender.
type RequestReadOnly interface {
	// Method returns HTTP method.
	Method() string
	// Url method returns HTTP URL.
	Url() string
	// RequestHeader method returns HTTP request headers.
	RequestHeader() http.Header
	// QueryParams method returns HTTP query parameters.
	QueryParams() url.Values
	// PathParams method returns HTTP path parameters mapped to a {placeholder} in the URL.
	PathParams() map[string]string
	// FormData method returns Form parameters.
	FormData() url.Values
	// Body method returns definition of HTTP request body.
	// Supported request body data types is
	// `string`, `[]byte`, `struct`, `map`, `slice` and `io.Reader`. Body value can be pointer or non-pointer.
	// Automatic marshalling for JSON and XML content type, if it is `struct`, `map`, or `slice`.
	Body() interface{}
	// IsSent method returns if the request has been sent.
	IsSent() bool
	// IsDone method returns if the request has been completed.
	IsDone() bool
}

// Response is HTTP response whose result is mapped to type R.
type Response[R Result] interface {
	RequestReadOnly
	// ResponseHeader method returns HTTP response headers.
	ResponseHeader() http.Header
	// StatusCode method returns HTTP status code.
	StatusCode() int
	// RawResponse method returns native HTTP response
	RawResponse() *http.Response
	// IsSuccess method returns true if HTTP status `code >= 200 and <= 299` otherwise false.
	IsSuccess() bool
	// IsError method returns true if HTTP status `code >= 400` otherwise false.
	IsError() bool
	// HasError method returns true is Error() is not nil.
	HasError() bool
	// Result method returns the response value mapped as a data type.
	Result() R
	// Error method returns the error response mapped as a data type, if any.
	// It can also return native HTTP errors, e.g. some network problem.
	Error() error
}

// NewRequest creates new HTTP request whose response should be mapped to type R.
// Value "resultDef" can be pointer or non-pointer value.
func NewRequest[R Result](resultDef R) Request[R] {
	return &request[R]{resultDef: resultDef}
}

// Implementation of the interfaces.

type request[R any] struct {
	requestData
	resultDef R
}

type requestData struct {
	sent        bool
	done        bool
	method      string
	url         *url.URL
	header      http.Header
	queryParams url.Values
	pathParams  map[string]string
	formData    url.Values
	body        interface{}
	errorDef    error
}

type requestEvents struct {
}

type response[R any] struct {
	*request[R]
	rawResponse *http.Response
	result      R
	error       error
}

func (r requestData) Method() string {
	if r.method == "" {
		panic(fmt.Errorf("request method is not set"))
	}
	return r.method
}

func (r requestData) Url() string {
	if r.url == nil {
		panic(fmt.Errorf("request url is not set"))
	}
	return r.url.String()
}

func (r requestData) RequestHeader() http.Header {
	return r.header
}

func (r requestData) QueryParams() url.Values {
	return r.queryParams
}

func (r requestData) PathParams() map[string]string {
	return r.pathParams
}

func (r requestData) FormData() url.Values {
	return r.formData
}

func (r requestData) Body() interface{} {
	return r.body
}

func (r requestData) IsSent() bool {
	return r.sent
}

func (r requestData) IsDone() bool {
	return r.done
}

func (r *request[R]) SetMethod(method string) Request[R] {
	if r.sent {
		panic(fmt.Errorf("sent request cannot be modified"))
	}
	r.method = method
	return r
}

func (r *request[R]) SetUrl(urlStr string) Request[R] {
	if r.sent {
		panic(fmt.Errorf("sent request cannot be modified"))
	}
	if v, err := url.Parse(urlStr); err == nil {
		r.url = v
	} else {
		panic(fmt.Errorf(`url "%s" is not valid :%w`, urlStr, err))
	}
	return r
}

func (r *request[R]) SetHeader(header string, value string) Request[R] {
	if r.sent {
		panic(fmt.Errorf("sent request cannot be modified"))
	}
	r.header.Set(header, value)
	return r
}

func (r *request[R]) SetQueryParam(param, value string) Request[R] {
	if r.sent {
		panic(fmt.Errorf("sent request cannot be modified"))
	}
	r.queryParams.Set(param, value)
	return r
}

func (r *request[R]) SetQueryParams(params map[string]string) Request[R] {
	for p, v := range params {
		r.SetQueryParam(p, v)
	}
	return r
}

func (r *request[R]) SetPathParam(param, value string) Request[R] {
	if r.sent {
		panic(fmt.Errorf("sent request cannot be modified"))
	}
	if r.pathParams == nil {
		r.pathParams = make(map[string]string)
	}
	r.pathParams[param] = value
	return r
}

func (r *request[R]) SetPathParams(params map[string]string) Request[R] {
	for p, v := range params {
		r.SetPathParam(p, v)
	}
	return r
}

func (r *request[R]) SetFormBody(body map[string]string) Request[R] {
	if r.sent {
		panic(fmt.Errorf("sent request cannot be modified"))
	}
	r.SetHeader("Content-Type", "application/x-www-form-urlencoded")
	for k, v := range body {
		r.formData.Set(k, v)
	}
	return r
}

func (r *request[R]) SetJsonBody(body map[string]string) Request[R] {
	if r.sent {
		panic(fmt.Errorf("sent request cannot be modified"))
	}
	r.SetHeader("Content-Type", "application/json")
	r.body = body // nolint
	return r
}

func (r *request[R]) SetErrorDef(err error) Request[R] {
	r.errorDef = err
	return r
}

func (r *request[R]) Send(sender Sender) Response[R] {
	r.sent = true
	rawResponse, result, err := sender.Send(r.requestData, r.resultDef, r.errorDef)
	r.done = true
	// Type of the return value "result" must be the same as type of the argument "r.resultDef", otherwise panic will occur.
	return &response[R]{request: r, rawResponse: rawResponse, result: result.(R), err: err}
}

func (r *response[T]) ResponseHeader() http.Header {
	return r.rawResponse.Header
}

func (r *response[T]) StatusCode() int {
	return r.rawResponse.StatusCode
}

func (r *response[T]) RawResponse() *http.Response {
	return r.rawResponse
}

func (r *response[T]) IsSuccess() bool {
	return r.StatusCode() > 199 && r.StatusCode() < 300
}

func (r *response[T]) IsError() bool {
	return r.StatusCode() > 399
}

func (r *response[T]) HasError() bool {
	return r.error != nil
}

func (r *response[T]) Result() T {
	return r.result
}

func (r *response[T]) Error() error {
	return r.error
}
