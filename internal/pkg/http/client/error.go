package client

import "github.com/go-resty/resty/v2"

type ErrorWithResponse interface {
	SetResponse(response *resty.Response)
	StatusCode() int
}
