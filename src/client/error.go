package client

import "github.com/go-resty/resty/v2"

type ErrorWithResponse interface {
	SetResponse(response *resty.Response)
	HttpStatus() int
	IsBadRequest() bool
	IsUnauthorized() bool
	IsForbidden() bool
	IsNotFound() bool
}
