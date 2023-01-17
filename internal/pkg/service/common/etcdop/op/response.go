package op

import (
	etcd "go.etcd.io/etcd/client/v3"
)

// Response wraps etcdOpResponse and adds client and options with which the request was made.
// The Response is used by the MapResponse methods.
type Response struct {
	etcd.OpResponse
	Client  etcd.KV
	Options []Option
}
