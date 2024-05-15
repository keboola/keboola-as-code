package op_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestTxnOp_FirstErrorOnly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))

	newOpWithErr := func(i int) op.Op {
		return etcdop.
			Key("key/"+strconv.Itoa(i)).
			Put(client, "value").
			WithProcessor(func(_ context.Context, r *op.Result[op.NoResult]) {
				r.AddErr(errors.New("error" + strconv.Itoa(i)))
			})
	}

	err := op.Txn(client).
		Merge(
			op.Txn(client).
				Then(newOpWithErr(1)).
				Merge(op.Txn(client).Then(newOpWithErr(2))).
				Then(newOpWithErr(3)),
		).
		Then(newOpWithErr(4)).
		Then(newOpWithErr(5)).
		FirstErrorOnly().
		Do(ctx).
		Err()

	assert.Equal(t, "error1", err.Error())
}
