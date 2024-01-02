package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// TxnOp provides a high-level interface built on top of etcd.Txn., see If, Then and Else methods.
//
// For more information on etcd transactions, please refer to:
// https://etcd.io/docs/v3.5/learning/api/#transaction
//
// Like other operations in this package, you can define processors using the AddProcessor method.
//
// High-level TxnOp is composed of high-level Op operations.
// This allows you to easily combine operations into an atomic transaction
// Processors defined in the operations will be executed.
//
// Another advantage is the ability to combine several TxnOp transactions into one, see Add method.
type TxnOp struct {
	client     etcd.KV
	processors []txnProcessor
	ifs        []etcd.Cmp
	thenOps    []Op
	elseOps    []Op
	andOps     []Op
}

type lowLevelTxn struct {
	client      etcd.KV
	processors  []txnProcessor
	ifs         []etcd.Cmp
	thenOps     []etcd.Op
	elseOps     []etcd.Op
	thenMappers []MapFn
	elseMappers []MapFn
}

type txnProcessor func(ctx context.Context, r *TxnResult)

// NewTxnOp creates an empty transaction.
func NewTxnOp(client etcd.KV) *TxnOp {
	return &TxnOp{client: client}
}

// MergeToTxn merges listed operations into a transaction using And method.
func MergeToTxn(client etcd.KV, ops ...Op) *TxnOp {
	return NewTxnOp(client).And(ops...)
}

// Then takes a list of operations.
// The operations will be executed, if the comparisons passed in If() succeed.
func (v *TxnOp) Then(ops ...Op) *TxnOp {
	v.thenOps = append(v.thenOps, ops...)
	return v
}

func (v *TxnOp) Empty() bool {
	return len(v.ifs) == 0 && len(v.thenOps) == 0 && len(v.elseOps) == 0 && len(v.andOps) == 0
}

// If takes a list of comparison.
// If all comparisons passed in succeed, the operations passed into Then() will be executed,
// otherwise the operations passed into Else() will be executed.
func (v *TxnOp) If(cs ...etcd.Cmp) *TxnOp {
	v.ifs = append(v.ifs, cs...)
	return v
}

// Else takes a list of operations.
// The operations list will be executed, if any from comparisons passed in If() fail.
func (v *TxnOp) Else(ops ...Op) *TxnOp {
	v.elseOps = append(v.elseOps, ops...)
	return v
}

// And merges the transaction with one or more other transactions.
// IF conditions from all transactions are merged and must be fulfilled, to invoke the Then branch,
// otherwise the Else branch is executed.
// The processor from all transactions are preserved and executed.
// For non-transactions operations, the method behaves same as the Then.
func (v *TxnOp) And(ops ...Op) *TxnOp {
	v.andOps = append(v.andOps, ops...)
	return v
}

// AddProcessor adds a processor callback which is always executed after the transaction.
func (v *TxnOp) AddProcessor(p txnProcessor) *TxnOp {
	v.processors = append(v.processors, p)
	return v
}

// OnResult is a shortcut for the AddProcessor.
// If no error occurred yet, then the callback is executed with the result.
func (v *TxnOp) OnResult(fn func(result *TxnResult)) *TxnOp {
	return v.AddProcessor(func(_ context.Context, r *TxnResult) {
		if r.Err() == nil {
			fn(r)
		}
	})
}

func (v *TxnOp) Do(ctx context.Context, opts ...Option) *TxnResult {
	if lowLevel, err := v.lowLevelTxn(ctx); err == nil {
		return lowLevel.Do(ctx, opts...)
	} else {
		return newTxnResult(nil).AddErr(err)
	}
}

func (v *TxnOp) Op(ctx context.Context) (LowLevelOp, error) {
	if lowLevel, err := v.lowLevelTxn(ctx); err == nil {
		return lowLevel.Op(ctx)
	} else {
		return LowLevelOp{}, err
	}
}

func (v *TxnOp) lowLevelTxn(ctx context.Context) (*lowLevelTxn, error) {
	out := &lowLevelTxn{client: v.client, thenOps: make([]etcd.Op, 0), elseOps: make([]etcd.Op, 0)}
	errs := errors.NewMultiError()

	// Copy processors
	out.processors = make([]txnProcessor, len(v.processors))
	copy(out.processors, v.processors)

	// Copy IFs
	out.ifs = make([]etcd.Cmp, len(v.ifs))
	copy(out.ifs, v.ifs)

	// Map THEN operations
	for i, op := range v.thenOps {
		// Create low-level operation
		if lowLevel, err := op.Op(ctx); err == nil {
			out.addThen(lowLevel.Op, lowLevel.MapResponse)
		} else {
			errs.Append(errors.PrefixErrorf(err, "cannot create operation [then][%d]", i))
		}
	}

	// Map ELSE operations
	for i, op := range v.elseOps {
		// Create low-level operation
		if lowLevel, err := op.Op(ctx); err == nil {
			out.addElse(lowLevel.Op, lowLevel.MapResponse)
		} else {
			errs.Append(errors.PrefixErrorf(err, "cannot create operation [else][%d]", i))
		}
	}

	// Map AND operations, merge transactions
	for i, op := range v.andOps {
		// Create low-level operation
		lowLevel, err := op.Op(ctx)
		if err != nil {
			errs.Append(errors.PrefixErrorf(err, "cannot create operation [and][%d]", i))
			continue
		}

		// If it is not a transaction, process it as Then
		if !lowLevel.Op.IsTxn() {
			out.addThen(lowLevel.Op, lowLevel.MapResponse)
			continue
		}

		// Get transaction parts
		ifs, thenOps, elseOps := lowLevel.Op.Txn()

		// Merge IFs
		out.ifs = append(out.ifs, ifs...)

		// Merge THEN operations
		// The THEN branch will be applied, if all conditions (from all sub-transactions) are met.
		thenStart := len(out.thenOps)
		thenEnd := thenStart + len(thenOps)
		for _, item := range thenOps {
			out.addThen(item, nil)
		}

		// Merge ELSE operations
		// The ELSE branch will be applied only if the conditions of the sub-transaction are not met
		elsePos := out.addElse(etcd.OpTxn(ifs, []etcd.Op{}, elseOps), nil)

		// There may be a situation where neither THEN nor ELSE branch is executed:
		// If the transaction fails, but the reason is not in this sub-transaction.

		// On result, compose and map response that corresponds to the original sub-transaction
		out.processors = append(out.processors, func(ctx context.Context, r *TxnResult) {
			// Get sub-transaction response
			var subTxnResponse *etcd.TxnResponse
			if r.succeeded {
				subTxnResponse = &etcd.TxnResponse{
					// The entire transaction succeeded, which means that a partial transaction succeeded as well
					Succeeded: true,
					// Compose responses that corresponds to the original sub-transaction
					Responses: r.response.Txn().Responses[thenStart:thenEnd],
				}
			} else {
				subTxnResponse = (*etcd.TxnResponse)(r.response.Txn().Responses[elsePos].GetResponseTxn())
				if subTxnResponse.Succeeded {
					// Skip mapper bellow, the transaction failed, but not due to a condition in the sub-transaction.
					r.AddResult(NoResult{})
					return
				}
			}

			// Call original mapper of the sub transaction
			if subResult, err := lowLevel.MapResponse(ctx, r.response.SubResponse(subTxnResponse.OpResponse())); err == nil {
				r.AddResult(subResult)
			} else {
				r.AddResult(err).AddErr(err)
			}
		})
	}

	if err := errs.ErrorOrNil(); err != nil {
		return nil, err
	}

	return out, nil
}

func (v *lowLevelTxn) Op(_ context.Context) (LowLevelOp, error) {
	return v.op(), nil
}

func (v *lowLevelTxn) op() LowLevelOp {
	return LowLevelOp{
		Op: etcd.OpTxn(v.ifs, v.thenOps, v.elseOps),
		MapResponse: func(ctx context.Context, raw RawResponse) (result any, err error) {
			txnResult := v.mapResponse(ctx, raw)
			return txnResult, txnResult.Err()
		},
	}
}

func (v *lowLevelTxn) Do(ctx context.Context, opts ...Option) *TxnResult {
	// Create low-level operation
	op := v.op()

	// Do with retry
	response, err := DoWithRetry(ctx, v.client, op.Op, opts...)
	if err != nil {
		return newTxnResult(nil).AddErr(err)
	}

	return v.mapResponse(ctx, response)
}

func (v *lowLevelTxn) addThen(op etcd.Op, mapper MapFn) {
	v.thenOps = append(v.thenOps, op)
	v.thenMappers = append(v.thenMappers, mapper)
}

func (v *lowLevelTxn) addElse(op etcd.Op, mapper MapFn) (index int) {
	index = len(v.elseOps)
	v.elseOps = append(v.elseOps, op)
	v.elseMappers = append(v.elseMappers, mapper)
	return index
}

func (v *lowLevelTxn) mapResponse(ctx context.Context, raw RawResponse) *TxnResult {
	// Map transaction response
	r := newTxnResult(&raw)
	r.succeeded = raw.Txn().Succeeded

	// Map sub-responses
	for i, subResponse := range raw.Txn().Responses {
		// Get mapper
		var mapper MapFn
		if r.succeeded {
			mapper = v.thenMappers[i]
		} else {
			mapper = v.elseMappers[i]
		}

		// Use mapper
		if mapper != nil {
			if subResult, err := mapper(ctx, raw.SubResponse(mapRawResponse(subResponse))); err == nil {
				r.AddResult(subResult)
			} else {
				r.AddErr(err)
			}
		}
	}

	// Use processors
	for _, p := range v.processors {
		p(ctx, r)
	}

	return r
}
