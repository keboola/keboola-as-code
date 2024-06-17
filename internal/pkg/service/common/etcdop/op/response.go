package op

import etcd "go.etcd.io/etcd/client/v3"

type response = etcd.OpResponse

type RawResponse struct {
	response
	client  etcd.KV
	options []Option
}

func newRawResponse(client etcd.KV, opts []Option) *RawResponse {
	return &RawResponse{client: client, options: opts}
}

func (v *RawResponse) WithOpResponse(r etcd.OpResponse) *RawResponse {
	clone := *v
	clone.response = r
	return &clone
}

func (v *RawResponse) Client() etcd.KV {
	return v.client
}

func (v *RawResponse) Options() []Option {
	return v.options
}

func (v *RawResponse) OpResponse() etcd.OpResponse {
	return v.response
}
