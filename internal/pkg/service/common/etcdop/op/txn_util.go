package op

import (
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// mapRawResponse maps types received from the etcd client.
func mapRawResponse(r *etcdserverpb.ResponseOp) etcd.OpResponse {
	switch {
	case r.GetResponseRange() != nil:
		return (*etcd.GetResponse)(r.GetResponseRange()).OpResponse()
	case r.GetResponsePut() != nil:
		return (*etcd.PutResponse)(r.GetResponsePut()).OpResponse()
	case r.GetResponseDeleteRange() != nil:
		return (*etcd.DeleteResponse)(r.GetResponseDeleteRange()).OpResponse()
	case r.GetResponseTxn() != nil:
		return (*etcd.TxnResponse)(r.GetResponseTxn()).OpResponse()
	default:
		panic(errors.New("no response found"))
	}
}
