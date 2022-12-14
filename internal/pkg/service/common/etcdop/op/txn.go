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
	cmps     []etcd.Cmp
	thenOps  []Op
	elseOps  []Op
	initErrs errors.MultiError
}

// TxnResponse with mapped response.
// Response type differs according to the used operation.
type TxnResponse struct {
	Succeeded bool
	Responses []any
}

// inlineOp is helper for NewTxnOp, it implements Op interface.
type inlineOp struct {
	op          etcd.Op
	mapResponse mapResponseFn
}

type mapResponseFn func(ctx context.Context, response etcd.OpResponse) (result any, err error)

func newInlineOp(op etcd.Op, fn mapResponseFn) Op {
	return &inlineOp{op: op, mapResponse: fn}
}

func (v *inlineOp) Op(_ context.Context) (etcd.Op, error) {
	return v.op, nil
}

func (v *inlineOp) MapResponse(ctx context.Context, response etcd.OpResponse) (result any, err error) {
	return v.mapResponse(ctx, response)
}

func NewTxnOp() *TxnOp {
	return &TxnOp{initErrs: errors.NewMultiError()}
}

func MergeToTxn(ctx context.Context, ops ...Op) *TxnOp {
	txn := NewTxnOp()
	for _, item := range ops {
		item := item

		// Convert high-level operation to low-level operation.
		// Error will be returned by the "Do" method.
		// It simplifies txn nesting. All errors are checked in the one place.
		op, err := item.Op(ctx)
		if err != nil {
			txn.initErrs.Append(err)
			continue
		}

		// Add operation to the "Then" branch, if it is not a sub-txn.
		if !op.IsTxn() {
			txn.Then(item)
			continue
		}

		// Transaction merging. See "TestMergeToTxn_Processors" for an example.
		subCmps, subThen, subElse := op.Txn()
		// Move "If" conditions from the sub-txn to the parent txn.
		// AND logic applies, so all merged "If" conditions must be met, to run all merged "Then" operations.
		txn.If(subCmps...)
		// Conditions are moved to the parent txn ^^^, so here are not needed.
		txn.Then(newInlineOp(etcd.OpTxn([]etcd.Cmp{}, subThen, []etcd.Op{}), item.MapResponse))
		// MapResponse/Processors for the "Else" branch of the sub-txn
		// should be called only if the sub-txn caused the parent txn fall.
		txn.Else(newInlineOp(etcd.OpTxn(subCmps, []etcd.Op{}, subElse), item.MapResponse))
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
	if err := v.initErrs.ErrorOrNil(); err != nil {
		return etcd.Op{}, err
	}

	errs := errors.NewMultiError()
	cmps := make([]etcd.Cmp, len(v.cmps))
	copy(cmps, v.cmps)

	thenOps := make([]etcd.Op, 0, len(v.thenOps))
	for i, op := range v.thenOps {
		etcdOp, err := op.Op(ctx)
		if err != nil {
			errs.Append(errors.Errorf("cannot create operation [then][%d]: %w", i, err))
			continue
		}
		thenOps = append(thenOps, etcdOp)
	}

	elseOps := make([]etcd.Op, 0, len(v.elseOps))
	for i, op := range v.elseOps {
		etcdOp, err := op.Op(ctx)
		if err != nil {
			errs.Append(errors.Errorf("cannot create operation [else][%d]: %w", i, err))
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
				errs.Append(subErr)
			}
			result.Responses = append(result.Responses, subResult)
		}
	} else {
		for i, subResponse := range r.Responses {
			subResult, subErr := v.elseOps[i].MapResponse(ctx, toOpResponse(subResponse))
			if subErr != nil {
				errs.Append(subErr)
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
