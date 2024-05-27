package op

import etcd "go.etcd.io/etcd/client/v3"

type RawResponse struct {
	etcd.OpResponse
	client  etcd.KV
	options []Option
}

func newRawResponse(client etcd.KV, opts []Option) *RawResponse {
	return &RawResponse{client: client, options: opts}
}

func (v RawResponse) Client() etcd.KV {
	return v.client
}

func (v RawResponse) Options() []Option {
	return v.options
}

func (v RawResponse) SubResponse(response etcd.OpResponse) *RawResponse {
	// copy Client and Options
	v.OpResponse = response
	return &v
}
