package op

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestMapRawResponse_Panic(t *testing.T) {
	t.Parallel()
	assert.PanicsWithError(t, "no response found", func() {
		mapRawResponse(&etcdserverpb.ResponseOp{})
	})
}
