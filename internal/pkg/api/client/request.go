package client

import (
	"fmt"
	"net/http"
	"net/url"
)

type Result = any

type NoResult struct{}

// HttpRequest contains read only request data used by the Sender.
type HttpRequest interface {
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
}

// HttpResponse is HTTP response whose result is mapped to type R.
type HttpResponse interface {
	HttpRequest
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
	// Error method returns the error response mapped as a data type, if any.
	// It can also return native HTTP errors, e.g. some network problem.
	Error() error
}

// Sender is implementation of HTTP client, e.g. using go-resty/resty lib, or native net/http.
type Sender interface {
	// Send method sends defined request and returns response.
	// Type of the return value "result" must be the same as type of the argument "resultDef", otherwise panic will occur.
	//   In Go, this rule cannot be written using generic types yet, methods cannot have generic types.
	//   Send[R Result](request HttpRequest, resultDef R, errorDef error) (result R, error error)
	Send(request HttpRequest, resultDef interface{}, errorDef error) (rawResponse *http.Response, result interface{}, error error)
}

type Request[R Result] interface {
	HttpRequest
	// WithMethod method sets the HTTP method.
	WithMethod(method string) Request[R]
	// WithUrl method sets the URL.
	WithUrl(url string) Request[R]
	// AndHeader method sets a single header field and its value.
	AndHeader(header string, value string) Request[R]
	// AndQueryParam method sets single parameter and its value.
	AndQueryParam(param, value string) Request[R]
	// WithQueryParams method sets multiple parameters and its values.
	WithQueryParams(params map[string]string) Request[R]
	// AndPathParam method sets single URL path key-value pair.
	AndPathParam(param, value string) Request[R]
	// WithPathParams method sets multiple URL path key-value pairs.
	WithPathParams(params map[string]string) Request[R]
	// WithFormBody method sets Form parameters and Content-Type header to "application/x-www-form-urlencoded".
	WithFormBody(body map[string]string) Request[R]
	// WithJsonBody method sets request body to a JSON value and Content-Type header to "application/json".
	WithJsonBody(body map[string]string) Request[R]
	// WithErrorDef method is to register the request `Error` value for automatic mapping.
	WithErrorDef(err error) Request[R]
	// WithOnComplete method registers callback to be executed when the request is completed.
	WithOnComplete(func(sender Sender, response HttpResponse) error) Request[R]
	// WithOnSuccess method registers callback to be executed when the request is completed and `code >= 200 and <= 299`.
	WithOnSuccess(func(sender Sender, response HttpResponse) error) Request[R]
	// WithOnError method registers callback to be executed when the request is completed and `code >= 400`.
	WithOnError(func(sender Sender, response HttpResponse) error) Request[R]
	// Send sends the request by the sender.
	Send(sender Sender) (response HttpResponse, result R, err error)
}

type Response[R Result] interface {
	HttpResponse
	// Result method returns the response value mapped as a data type.
	Result() R
}

// Implementation of the interfaces.

// NewRequest creates new HTTP request whose response should be mapped to type R.
// Value "resultDef" can be pointer or non-pointer value.
func NewRequest[R Result](resultDef R) Request[R] {
	return request[R]{resultDef: resultDef}
}

type request[R Result] struct {
	method      string
	url         *url.URL
	header      http.Header
	queryParams url.Values
	pathParams  map[string]string
	formData    url.Values
	body        interface{}
	resultDef   R
	errorDef    error
}

type httpRequest = HttpRequest
type response[R Result] struct {
	httpRequest
	rawResponse *http.Response
	result      R
	err         error
}

func (r request[R]) Method() string {
	if r.method == "" {
		panic(fmt.Errorf("request method is not set"))
	}
	return r.method
}

func (r request[R]) Url() string {
	if r.url == nil {
		panic(fmt.Errorf("request url is not set"))
	}
	return r.url.String()
}

func (r request[R]) RequestHeader() http.Header {
	return r.header
}

func (r request[R]) QueryParams() url.Values {
	return r.queryParams
}

func (r request[R]) PathParams() map[string]string {
	return r.pathParams
}

func (r request[R]) FormData() url.Values {
	return r.formData
}

func (r request[R]) Body() interface{} {
	return r.body
}

func (r request[R]) WithMethod(method string) Request[R] {
	r.method = method
	return r
}

func (r request[R]) WithUrl(urlStr string) Request[R] {
	if v, err := url.Parse(urlStr); err == nil {
		r.url = v
	} else {
		panic(fmt.Errorf(`url "%s" is not valid :%w`, urlStr, err))
	}
	return r
}

func (r request[R]) AndHeader(header string, value string) Request[R] {
	r.header = r.header.Clone()
	r.header.Set(header, value)
	return r
}

func (r request[R]) AndQueryParam(key, value string) Request[R] {
	r.queryParams = cloneUrlValues(r.queryParams)
	r.queryParams.Set(key, value)
	return r
}

func (r request[R]) WithQueryParams(params map[string]string) Request[R] {
	r.queryParams = make(url.Values)
	for k, v := range params {
		r.queryParams.Set(k, v)
	}
	return r
}

func (r request[R]) AndPathParam(key, value string) Request[R] {
	r.pathParams = cloneParams(r.pathParams)
	r.pathParams[key] = value
	return r
}

func (r request[R]) WithPathParams(params map[string]string) Request[R] {
	r.pathParams = make(map[string]string)
	for k, v := range params {
		r.pathParams[k] = v
	}
	return r
}

func (r request[R]) WithFormBody(body map[string]string) Request[R] {
	r.formData = make(url.Values)
	for k, v := range body {
		r.formData.Set(k, v)
	}
	return r.AndHeader("Content-Type", "application/x-www-form-urlencoded")
}

func (r request[R]) WithJsonBody(body map[string]string) Request[R] {
	r.body = body
	return r.AndHeader("Content-Type", "application/json")
}

func (r request[R]) WithErrorDef(err error) Request[R] {
	r.errorDef = err
	return r
}

func (r request[R]) WithOnComplete(func(sender Sender, response HttpResponse) error) Request[R] {
	panic(fmt.Errorf("not implemented"))
}

func (r request[R]) WithOnSuccess(func(sender Sender, response HttpResponse) error) Request[R] {
	panic(fmt.Errorf("not implemented"))
}

func (r request[R]) WithOnError(func(sender Sender, response HttpResponse) error) Request[R] {
	panic(fmt.Errorf("not implemented"))
}

// >>>>>>>>>>>> Type of the return value "result" must be the same as type of the argument "r.resultDef", otherwise panic will occur.

func (r request[R]) Send(sender Sender) (HttpResponse, R, error) {
	rawResponse, result, err := sender.Send(r, r.resultDef, r.errorDef)
	out := &response{httpRequest: r, rawResponse: rawResponse, result: result, err: err}
	return out, out.result, out.err
}

func (r *response[R]) ResponseHeader() http.Header {
	return r.rawResponse.Header
}

func (r *response[R]) StatusCode() int {
	return r.rawResponse.StatusCode
}

func (r *response[R]) RawResponse() *http.Response {
	return r.rawResponse
}

func (r *response[R]) IsSuccess() bool {
	return r.StatusCode() > 199 && r.StatusCode() < 300
}

func (r *response[R]) IsError() bool {
	return r.StatusCode() > 399
}

func (r *response[R]) Result() interface{} {
	return r.result
}

func (r *response[R]) Error() error {
	return r.err
}

func cloneParams(in map[string]string) (out map[string]string) {
	out = make(map[string]string)
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneUrlValues(in url.Values) (out url.Values) {
	out = make(url.Values)
	for k, values := range in {
		for _, v := range values {
			out.Add(k, v)
		}
	}
	return out
}
