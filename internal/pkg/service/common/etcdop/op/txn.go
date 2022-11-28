package op

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// TxnOp provides high-level interface above etcd.Txn.
// Values are mapped by the op.mapper operations
// and processed by the op.processors, see ForType[R] type.
type TxnOp struct {
	cmps    []etcd.Cmp
	thenOps []Op
	elseOps []Op
}

// TxnResponse with mapped response.
// Response type differs according to the used operation.
type TxnResponse struct {
	Succeeded bool
	Responses []any
}

func NewTxnOp() *TxnOp {
	return &TxnOp{}
}

func MergeToTxn(ops ...Op) *TxnOp {
	txn := NewTxnOp()
	for _, op := range ops {
		if v, ok := op.(*TxnOp); ok {
			// Merge a sub txn with the txn.
			// If the conditions use the AND operator,
			// they must all be fulfilled, so just connect them.
			txn.If(v.cmps...)
			txn.Then(v.thenOps...)
			txn.Else(v.elseOps...)
		} else {
			txn.Then(op)
		}
	}
	return txn
}

// If takes a list of comparison. If all comparisons passed in succeed,
// the operations passed into Then() will be executed. Or the operations
// passed into Else() will be executed.
func (v *TxnOp) If(cs ...etcd.Cmp) *TxnOp {
	v.cmps = append(v.cmps, cs...)
	return v
}

// Then takes a list of operations. The Ops list will be executed, if the
// comparisons passed in If() succeed.
func (v *TxnOp) Then(ops ...Op) *TxnOp {
	v.thenOps = append(v.thenOps, ops...)
	return v
}

// Else takes a list of operations. The Ops list will be executed, if the
// comparisons passed in If() fail.
func (v *TxnOp) Else(ops ...Op) *TxnOp {
	v.elseOps = append(v.elseOps, ops...)
	return v
}

func (v *TxnOp) Do(ctx context.Context, client *etcd.Client) (TxnResponse, error) {
	op, err := v.Op(ctx)
	if err != nil {
		return TxnResponse{}, err
	}

	response, err := client.Do(ctx, op)
	if err != nil {
		return TxnResponse{}, err
	}

	return v.mapResponse(ctx, response)
}

func (v *TxnOp) Op(ctx context.Context) (etcd.Op, error) {
	errs := errors.NewMultiError()

	cmps := make([]etcd.Cmp, len(v.cmps))
	copy(cmps, v.cmps)

	thenOps := make([]etcd.Op, 0, len(v.thenOps))
	for i, op := range v.thenOps {
		etcdOp, err := op.Op(ctx)
		if err != nil {
			errs.Append(errors.Errorf("cannot create etcd op for txn [then][%d]: %w", i, err))
			continue
		}
		thenOps = append(thenOps, etcdOp)
	}

	elseOps := make([]etcd.Op, 0, len(v.elseOps))
	for i, op := range v.elseOps {
		etcdOp, err := op.Op(ctx)
		if err != nil {
			errs.Append(errors.Errorf("cannot create etcd op for txn [else][%d]: %w", i, err))
			continue
		}
		elseOps = append(elseOps, etcdOp)
	}

	if errs.Len() > 0 {
		return etcd.Op{}, errs.ErrorOrNil()
	}

	return etcd.OpTxn(cmps, thenOps, elseOps), nil
}

func (v *TxnOp) MapResponse(ctx context.Context, response etcd.OpResponse) (result any, err error) {
	return v.mapResponse(ctx, response)
}

func (v *TxnOp) mapResponse(ctx context.Context, response etcd.OpResponse) (result TxnResponse, err error) {
	r := response.Txn()
	result.Succeeded = r.Succeeded

	errs := errors.NewMultiError()
	if r.Succeeded {
		for i, subResponse := range r.Responses {
			subResult, subErr := v.thenOps[i].MapResponse(ctx, toOpResponse(subResponse))
			if subErr != nil {
				errs.Append(errors.Errorf("cannot process etcd response from the transaction step [then][%d]: %w", i, subErr))
			}
			result.Responses = append(result.Responses, subResult)
		}
	} else {
		for i, subResponse := range r.Responses {
			subResult, subErr := v.elseOps[i].MapResponse(ctx, toOpResponse(subResponse))
			if subErr != nil {
				errs.Append(errors.Errorf("cannot process etcd response from the transaction step [else][%d]: %w", i, subErr))
			}
			result.Responses = append(result.Responses, subResult)
		}
	}

	if errs.Len() > 0 {
		return TxnResponse{}, errs.ErrorOrNil()
	}

	return result, nil
}

func toOpResponse(r *etcdserverpb.ResponseOp) etcd.OpResponse {
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
		panic(errors.Errorf(`unexpected type "%T"`, r))
	}
}
