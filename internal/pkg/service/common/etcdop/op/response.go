package op

import etcd "go.etcd.io/etcd/client/v3"

type RawResponse struct {
	etcd.OpResponse
	Client  etcd.KV
	Options []Option
}

func (v RawResponse) SubResponse(response etcd.OpResponse) RawResponse {
	v.OpResponse = response
	return v
}
