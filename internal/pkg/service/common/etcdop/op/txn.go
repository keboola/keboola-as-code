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
	cmps       []etcd.Cmp
	thenOps    []Op
	elseOps    []Op
	initErrs   errors.MultiError
	processors []txnProcessor
}

type txnProcessor func(ctx context.Context, rawResponse *etcd.TxnResponse, result TxnResult, err error) error

type TxnOpDef struct {
	ops        []Op
	processors []txnProcessor
}

// TxnResult with mapped partial results.
// Response type differs according to the used operation.
type TxnResult struct {
	Succeeded bool
	Header    *etcdserverpb.ResponseHeader
	Results   []any
}

// inlineOp is helper for NewTxnOp, it implements Op interface.
type inlineOp struct {
	op          etcd.Op
	err         error
	mapResponse mapResponseFn
}

type mapResponseFn func(ctx context.Context, response etcd.OpResponse) (result any, err error)

func newInlineOp(op etcd.Op, err error, fn mapResponseFn) Op {
	return &inlineOp{op: op, err: err, mapResponse: fn}
}

func (v *inlineOp) Op(_ context.Context) (etcd.Op, error) {
	return v.op, v.err
}

func (v *inlineOp) MapResponse(ctx context.Context, response etcd.OpResponse) (result any, err error) {
	return v.mapResponse(ctx, response)
}

func (v *inlineOp) DoWithHeader(ctx context.Context, client etcd.KV, opts ...Option) (*etcdserverpb.ResponseHeader, error) {
	op, err := v.Op(ctx)
	if err != nil {
		return nil, err
	}
	response, err := DoWithRetry(ctx, client, op, opts...)
	return getResponseHeader(response), err
}

func (v *inlineOp) DoOrErr(ctx context.Context, client etcd.KV, opts ...Option) error {
	_, err := v.DoWithHeader(ctx, client, opts...)
	return err
}

func NewTxnOp() *TxnOp {
	return &TxnOp{initErrs: errors.NewMultiError()}
}

func MergeToTxn(ops ...Op) *TxnOpDef {
	return &TxnOpDef{ops: ops}
}

func (v *TxnOpDef) Add(ops ...Op) *TxnOpDef {
	v.ops = append(v.ops, ops...)
	return v
}

func (v *TxnOpDef) WithProcessor(p txnProcessor) *TxnOpDef {
	v.processors = append(v.processors, p)
	return v
}

// WithOnResult is a shortcut for the WithProcessor.
func (v *TxnOpDef) WithOnResult(fn func(result TxnResult)) *TxnOpDef {
	return v.WithProcessor(func(_ context.Context, _ *etcd.TxnResponse, result TxnResult, err error) error {
		if err == nil {
			fn(result)
		}
		return err
	})
}

// WithOnResultOrErr is a shortcut for the WithProcessor.
func (v *TxnOpDef) WithOnResultOrErr(fn func(result TxnResult) error) *TxnOpDef {
	return v.WithProcessor(func(_ context.Context, _ *etcd.TxnResponse, result TxnResult, err error) error {
		if err == nil {
			err = fn(result)
		}
		return err
	})
}

func (v *TxnOpDef) Txn(ctx context.Context) *TxnOp {
	txn := NewTxnOp()
	for _, item := range v.ops {
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
		txn.Then(newInlineOp(etcd.OpTxn([]etcd.Cmp{}, subThen, []etcd.Op{}), nil, item.MapResponse))
		// MapResponse/Processors for the "Else" branch of the sub-txn
		// should be called only if the sub-txn caused the parent txn fall.
		txn.Else(newInlineOp(etcd.OpTxn(subCmps, []etcd.Op{}, subElse), nil, item.MapResponse))
	}

	txn.processors = v.processors[:]

	return txn
}

func (v *TxnOpDef) Do(ctx context.Context, client etcd.KV, opts ...Option) (TxnResult, error) {
	return v.Txn(ctx).Do(ctx, client, opts...)
}

func (v *TxnOpDef) DoWithHeader(ctx context.Context, client etcd.KV, opts ...Option) (*etcdserverpb.ResponseHeader, error) {
	return v.Txn(ctx).DoWithHeader(ctx, client, opts...)
}

func (v *TxnOpDef) DoOrErr(ctx context.Context, client etcd.KV, opts ...Option) error {
	return v.Txn(ctx).DoOrErr(ctx, client, opts...)
}

func (v *TxnOpDef) Op(ctx context.Context) (etcd.Op, error) {
	return v.Txn(ctx).Op(ctx)
}

func (v *TxnOpDef) MapResponse(ctx context.Context, response etcd.OpResponse) (result any, err error) {
	return v.Txn(ctx).MapResponse(ctx, response)
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

func (v *TxnOp) Do(ctx context.Context, client etcd.KV, opts ...Option) (TxnResult, error) {
	op, err := v.Op(ctx)
	if err != nil {
		return TxnResult{}, err
	}
	response, err := DoWithRetry(ctx, client, op, opts...)
	if err != nil {
		return TxnResult{}, err
	}

	return v.mapResponse(ctx, response)
}

func (v *TxnOp) DoWithHeader(ctx context.Context, client etcd.KV, opts ...Option) (*etcdserverpb.ResponseHeader, error) {
	r, err := v.Do(ctx, client, opts...)
	return r.Header, err
}

func (v *TxnOp) DoOrErr(ctx context.Context, client etcd.KV, opts ...Option) error {
	_, err := v.Do(ctx, client, opts...)
	return err
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

func (v *TxnOp) WithProcessor(p txnProcessor) *TxnOp {
	clone := *v
	clone.processors = append(clone.processors, p)
	return &clone
}

func (v *TxnOp) MapResponse(ctx context.Context, response etcd.OpResponse) (result any, err error) {
	return v.mapResponse(ctx, response)
}

func (v *TxnOp) mapResponse(ctx context.Context, response etcd.OpResponse) (result TxnResult, err error) {
	r := response.Txn()
	result.Header = r.Header
	result.Succeeded = r.Succeeded

	errs := errors.NewMultiError()
	if r.Succeeded {
		for i, subResponse := range r.Responses {
			subResult, subErr := v.thenOps[i].MapResponse(ctx, toOpResponse(subResponse))
			if subErr != nil {
				errs.Append(subErr)
			}
			result.Results = append(result.Results, subResult)
		}
	} else {
		for i, subResponse := range r.Responses {
			subResult, subErr := v.elseOps[i].MapResponse(ctx, toOpResponse(subResponse))
			if subErr != nil {
				errs.Append(subErr)
			}
			result.Results = append(result.Results, subResult)
		}
	}

	err = errs.ErrorOrNil()
	for _, p := range v.processors {
		err = p(ctx, response.Txn(), result, err)
	}

	if err != nil {
		return TxnResult{}, err
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
