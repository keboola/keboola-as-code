// Code generated by goa v3.19.1, DO NOT EDIT.
//
// apps-proxy HTTP client encoders and decoders
//
// Command:
// $ goa gen github.com/keboola/keboola-as-code/api/appsproxy --output
// ./internal/pkg/service/appsproxy/api

package client

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"

	appsproxy "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/api/gen/apps_proxy"
	goahttp "goa.design/goa/v3/http"
)

// BuildAPIRootIndexRequest instantiates a HTTP request object with method and
// path set to call the "apps-proxy" service "ApiRootIndex" endpoint
func (c *Client) BuildAPIRootIndexRequest(ctx context.Context, v any) (*http.Request, error) {
	u := &url.URL{Scheme: c.scheme, Host: c.host, Path: APIRootIndexAppsProxyPath()}
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, goahttp.ErrInvalidURL("apps-proxy", "ApiRootIndex", u.String(), err)
	}
	if ctx != nil {
		req = req.WithContext(ctx)
	}

	return req, nil
}

// DecodeAPIRootIndexResponse returns a decoder for responses returned by the
// apps-proxy ApiRootIndex endpoint. restoreBody controls whether the response
// body should be restored after having been read.
func DecodeAPIRootIndexResponse(decoder func(*http.Response) goahttp.Decoder, restoreBody bool) func(*http.Response) (any, error) {
	return func(resp *http.Response) (any, error) {
		if restoreBody {
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			resp.Body = io.NopCloser(bytes.NewBuffer(b))
			defer func() {
				resp.Body = io.NopCloser(bytes.NewBuffer(b))
			}()
		} else {
			defer resp.Body.Close()
		}
		switch resp.StatusCode {
		case http.StatusMovedPermanently:
			return nil, nil
		default:
			body, _ := io.ReadAll(resp.Body)
			return nil, goahttp.ErrInvalidResponse("apps-proxy", "ApiRootIndex", resp.StatusCode, string(body))
		}
	}
}

// BuildAPIVersionIndexRequest instantiates a HTTP request object with method
// and path set to call the "apps-proxy" service "ApiVersionIndex" endpoint
func (c *Client) BuildAPIVersionIndexRequest(ctx context.Context, v any) (*http.Request, error) {
	u := &url.URL{Scheme: c.scheme, Host: c.host, Path: APIVersionIndexAppsProxyPath()}
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, goahttp.ErrInvalidURL("apps-proxy", "ApiVersionIndex", u.String(), err)
	}
	if ctx != nil {
		req = req.WithContext(ctx)
	}

	return req, nil
}

// DecodeAPIVersionIndexResponse returns a decoder for responses returned by
// the apps-proxy ApiVersionIndex endpoint. restoreBody controls whether the
// response body should be restored after having been read.
func DecodeAPIVersionIndexResponse(decoder func(*http.Response) goahttp.Decoder, restoreBody bool) func(*http.Response) (any, error) {
	return func(resp *http.Response) (any, error) {
		if restoreBody {
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			resp.Body = io.NopCloser(bytes.NewBuffer(b))
			defer func() {
				resp.Body = io.NopCloser(bytes.NewBuffer(b))
			}()
		} else {
			defer resp.Body.Close()
		}
		switch resp.StatusCode {
		case http.StatusOK:
			var (
				body APIVersionIndexResponseBody
				err  error
			)
			err = decoder(resp).Decode(&body)
			if err != nil {
				return nil, goahttp.ErrDecodingError("apps-proxy", "ApiVersionIndex", err)
			}
			err = ValidateAPIVersionIndexResponseBody(&body)
			if err != nil {
				return nil, goahttp.ErrValidationError("apps-proxy", "ApiVersionIndex", err)
			}
			res := NewAPIVersionIndexServiceDetailOK(&body)
			return res, nil
		default:
			body, _ := io.ReadAll(resp.Body)
			return nil, goahttp.ErrInvalidResponse("apps-proxy", "ApiVersionIndex", resp.StatusCode, string(body))
		}
	}
}

// BuildHealthCheckRequest instantiates a HTTP request object with method and
// path set to call the "apps-proxy" service "HealthCheck" endpoint
func (c *Client) BuildHealthCheckRequest(ctx context.Context, v any) (*http.Request, error) {
	u := &url.URL{Scheme: c.scheme, Host: c.host, Path: HealthCheckAppsProxyPath()}
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, goahttp.ErrInvalidURL("apps-proxy", "HealthCheck", u.String(), err)
	}
	if ctx != nil {
		req = req.WithContext(ctx)
	}

	return req, nil
}

// DecodeHealthCheckResponse returns a decoder for responses returned by the
// apps-proxy HealthCheck endpoint. restoreBody controls whether the response
// body should be restored after having been read.
func DecodeHealthCheckResponse(decoder func(*http.Response) goahttp.Decoder, restoreBody bool) func(*http.Response) (any, error) {
	return func(resp *http.Response) (any, error) {
		if restoreBody {
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			resp.Body = io.NopCloser(bytes.NewBuffer(b))
			defer func() {
				resp.Body = io.NopCloser(bytes.NewBuffer(b))
			}()
		} else {
			defer resp.Body.Close()
		}
		switch resp.StatusCode {
		case http.StatusOK:
			var (
				body string
				err  error
			)
			err = decoder(resp).Decode(&body)
			if err != nil {
				return nil, goahttp.ErrDecodingError("apps-proxy", "HealthCheck", err)
			}
			return body, nil
		default:
			body, _ := io.ReadAll(resp.Body)
			return nil, goahttp.ErrInvalidResponse("apps-proxy", "HealthCheck", resp.StatusCode, string(body))
		}
	}
}

// BuildValidateRequest instantiates a HTTP request object with method and path
// set to call the "apps-proxy" service "Validate" endpoint
func (c *Client) BuildValidateRequest(ctx context.Context, v any) (*http.Request, error) {
	u := &url.URL{Scheme: c.scheme, Host: c.host, Path: ValidateAppsProxyPath()}
	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, goahttp.ErrInvalidURL("apps-proxy", "Validate", u.String(), err)
	}
	if ctx != nil {
		req = req.WithContext(ctx)
	}

	return req, nil
}

// EncodeValidateRequest returns an encoder for requests sent to the apps-proxy
// Validate server.
func EncodeValidateRequest(encoder func(*http.Request) goahttp.Encoder) func(*http.Request, any) error {
	return func(req *http.Request, v any) error {
		p, ok := v.(*appsproxy.ValidatePayload)
		if !ok {
			return goahttp.ErrInvalidType("apps-proxy", "Validate", "*appsproxy.ValidatePayload", v)
		}
		{
			head := p.StorageAPIToken
			req.Header.Set("X-StorageApi-Token", head)
		}
		return nil
	}
}

// DecodeValidateResponse returns a decoder for responses returned by the
// apps-proxy Validate endpoint. restoreBody controls whether the response body
// should be restored after having been read.
func DecodeValidateResponse(decoder func(*http.Response) goahttp.Decoder, restoreBody bool) func(*http.Response) (any, error) {
	return func(resp *http.Response) (any, error) {
		if restoreBody {
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}
			resp.Body = io.NopCloser(bytes.NewBuffer(b))
			defer func() {
				resp.Body = io.NopCloser(bytes.NewBuffer(b))
			}()
		} else {
			defer resp.Body.Close()
		}
		switch resp.StatusCode {
		case http.StatusOK:
			var (
				body ValidateResponseBody
				err  error
			)
			err = decoder(resp).Decode(&body)
			if err != nil {
				return nil, goahttp.ErrDecodingError("apps-proxy", "Validate", err)
			}
			err = ValidateValidateResponseBody(&body)
			if err != nil {
				return nil, goahttp.ErrValidationError("apps-proxy", "Validate", err)
			}
			res := NewValidateValidationsOK(&body)
			return res, nil
		default:
			body, _ := io.ReadAll(resp.Body)
			return nil, goahttp.ErrInvalidResponse("apps-proxy", "Validate", resp.StatusCode, string(body))
		}
	}
}

// unmarshalConfigurationResponseBodyToAppsproxyConfiguration builds a value of
// type *appsproxy.Configuration from a value of type
// *ConfigurationResponseBody.
func unmarshalConfigurationResponseBodyToAppsproxyConfiguration(v *ConfigurationResponseBody) *appsproxy.Configuration {
	if v == nil {
		return nil
	}
	res := &appsproxy.Configuration{
		ID:           *v.ID,
		ClientID:     *v.ClientID,
		ClientSecret: *v.ClientSecret,
	}

	return res
}
