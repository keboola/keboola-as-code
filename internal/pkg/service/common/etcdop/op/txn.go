package op

import (
	"context"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	txnOpIf      = txnPartType("if")
	txnOpThen    = txnPartType("then")
	txnOpThenTxn = txnPartType("thenTxn") // thenTxnOps are separated from thenOps to avoid confusing between Then and Merge
	txnOpElse    = txnPartType("else")
	txnOpMerge   = txnPartType("merge")
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
//
// R is type of the transaction result, use NoResult type if you don't need it.
// The results of individual sub-operations can be obtained using TxnResult.SubResults.
type TxnOp[R any] struct {
	result     *R
	client     etcd.KV
	errs       errors.MultiError
	parts      []txnPart[R]
	processors []func(ctx context.Context, r *TxnResult[R])
}

// txnInterface is a marker interface for generic type TxnOp.
type txnInterface interface {
	txn()
}

type txnPartType string

type txnPart[R any] struct {
	Type    txnPartType
	If      etcd.Cmp
	Factory Op
}

type lowLevelTxn[R any] struct {
	result     *R
	client     etcd.KV
	processors []func(ctx context.Context, r *TxnResult[R])
	opCounter  map[txnPartType]int
	ifs        []etcd.Cmp
	thenOps    []etcd.Op
	elseOps    []etcd.Op
}

// Txn creates an empty transaction with NoResult.
func Txn(client etcd.KV) *TxnOp[NoResult] {
	return &TxnOp[NoResult]{client: client, result: &NoResult{}}
}

// TxnWithResult creates an empty transaction with the result.
func TxnWithResult[R any](client etcd.KV, result *R) *TxnOp[R] {
	return &TxnOp[R]{client: client, result: result}
}

// MergeToTxn merges listed operations into a transaction using And method.
func MergeToTxn(client etcd.KV, ops ...Op) *TxnOp[NoResult] {
	return Txn(client).Merge(ops...)
}

// txn is a marker method defined by the txnInterface.
func (v *TxnOp[R]) txn() {}

func (v *TxnOp[R]) Empty() bool {
	return len(v.parts) == 0
}

// If takes a list of comparison.
// If all comparisons succeed, the Then branch will be executed; otherwise, the Else branch will be executed.
func (v *TxnOp[R]) If(cs ...etcd.Cmp) *TxnOp[R] {
	for _, c := range cs {
		v.parts = append(v.parts, txnPart[R]{Type: txnOpIf, If: c})
	}

	return v
}

// Then takes a list of operations.
// The Then operations will be executed if all If comparisons succeed.
// To add a transaction to the Then branch, use ThenTxn, or use Merge to merge transactions.
func (v *TxnOp[R]) Then(ops ...Op) *TxnOp[R] {
	for i, op := range ops {
		// Check common high-level transaction types.
		// Bulletproof check of the low-level transaction is in the "lowLevelTxn" method.
		if _, ok := op.(txnInterface); ok {
			panic(errors.Errorf(`invalid operation[%d]: op is a transaction, use Merge or ThenTxn, not Then`, i))
		}
		v.parts = append(v.parts, txnPart[R]{Type: txnOpThen, Factory: op})
	}

	return v
}

// ThenTxn adds the transaction to the Then branch.
// To merge a transaction, use Merge.
func (v *TxnOp[R]) ThenTxn(ops ...Op) *TxnOp[R] {
	for _, op := range ops {
		v.parts = append(v.parts, txnPart[R]{Type: txnOpThenTxn, Factory: op})
	}
	return v
}

// Else takes a list of operations.
// The operations in the Else branch will be executed if any of the If comparisons fail.
func (v *TxnOp[R]) Else(ops ...Op) *TxnOp[R] {
	for _, op := range ops {
		v.parts = append(v.parts, txnPart[R]{Type: txnOpElse, Factory: op})
	}
	return v
}

// Merge merges the transaction with one or more other transactions.
// If comparisons from all transactions are merged.
// The processors from all transactions are preserved and executed.
//
// For non-transaction operations, the method behaves the same as Then.
// To add a transaction to the Then branch without merging, use ThenTxn.
func (v *TxnOp[R]) Merge(ops ...Op) *TxnOp[R] {
	for _, op := range ops {
		v.parts = append(v.parts, txnPart[R]{Type: txnOpMerge, Factory: op})
	}
	return v
}

// AddError - all static errors are returned when the low level txn is composed.
// It makes error handling easier and move it to one place.
func (v *TxnOp[R]) AddError(errs ...error) *TxnOp[R] {
	if v.errs == nil {
		v.errs = errors.NewMultiError()
	}
	v.errs.Append(errs...)
	return v
}

// AddProcessor adds a processor callback which is always executed after the transaction.
func (v *TxnOp[R]) AddProcessor(p func(ctx context.Context, r *TxnResult[R])) *TxnOp[R] {
	v.processors = append(v.processors, p)
	return v
}

// OnResult is a shortcut for the AddProcessor.
// If no error occurred yet, then the callback is executed with the result.
func (v *TxnOp[R]) OnResult(fn func(result *TxnResult[R])) *TxnOp[R] {
	return v.AddProcessor(func(_ context.Context, r *TxnResult[R]) {
		if r.Err() == nil {
			fn(r)
		}
	})
}

// SetResultTo is a shortcut for the AddProcessor.
// If no error occurred, the result of the operation is written to the target pointer,
// otherwise an empty value is written.
func (v *TxnOp[R]) SetResultTo(ptr *R) *TxnOp[R] {
	v.AddProcessor(func(ctx context.Context, r *TxnResult[R]) {
		if r.Err() == nil {
			*ptr = r.Result()
		} else {
			var empty R
			*ptr = empty
		}
	})
	return v
}

// OnFailed is a shortcut for the AddProcessor.
// If no error occurred yet and the transaction is failed, then the callback is executed.
func (v *TxnOp[R]) OnFailed(fn func(result *TxnResult[R])) *TxnOp[R] {
	return v.AddProcessor(func(_ context.Context, r *TxnResult[R]) {
		if r.Err() == nil && !r.Succeeded() {
			fn(r)
		}
	})
}

// OnSucceeded is a shortcut for the AddProcessor.
// If no error occurred yet and the transaction is succeeded, then the callback is executed.
func (v *TxnOp[R]) OnSucceeded(fn func(result *TxnResult[R])) *TxnOp[R] {
	return v.AddProcessor(func(_ context.Context, r *TxnResult[R]) {
		if r.Err() == nil && r.Succeeded() {
			fn(r)
		}
	})
}

func (v *TxnOp[R]) Do(ctx context.Context, opts ...Option) *TxnResult[R] {
	if lowLevel, err := v.lowLevelTxn(ctx); err == nil {
		return lowLevel.Do(ctx, opts...)
	} else {
		return newErrorTxnResult[R](err)
	}
}

func (v *TxnOp[R]) Op(ctx context.Context) (LowLevelOp, error) {
	if lowLevel, err := v.lowLevelTxn(ctx); err == nil {
		return lowLevel.Op(ctx)
	} else {
		return LowLevelOp{}, err
	}
}

func (v *TxnOp[R]) lowLevelTxn(ctx context.Context) (*lowLevelTxn[R], error) {
	out := &lowLevelTxn[R]{
		result:    v.result,
		client:    v.client,
		opCounter: make(map[txnPartType]int),
		thenOps:   make([]etcd.Op, 0),
		elseOps:   make([]etcd.Op, 0),
	}

	errs := errors.NewMultiError()

	// Copy init errors
	if v.errs != nil {
		errs.Append(v.errs.WrappedErrors()...)
	}

	// Process all transaction parts
	for _, part := range v.parts {
		if err := out.addPart(ctx, part); err != nil {
			errs.Append(err)
		}
	}

	// Add top-level processors
	out.processors = append(out.processors, v.processors...)

	if err := errs.ErrorOrNil(); err != nil {
		return nil, err
	}

	return out, nil
}

func (v *lowLevelTxn[R]) Op(_ context.Context) (LowLevelOp, error) {
	return v.op(), nil
}

func (v *lowLevelTxn[R]) op() LowLevelOp {
	return LowLevelOp{
		Op: etcd.OpTxn(v.ifs, v.thenOps, v.elseOps),
		MapResponse: func(ctx context.Context, raw RawResponse) (result any, err error) {
			txnResult := v.mapResponse(ctx, raw)
			return txnResult, txnResult.Err()
		},
	}
}

func (v *lowLevelTxn[R]) Do(ctx context.Context, opts ...Option) *TxnResult[R] {
	// Create low-level operation
	op := v.op()

	// Do with retry
	response, err := DoWithRetry(ctx, v.client, op.Op, opts...)
	if err != nil {
		return newErrorTxnResult[R](err)
	}

	return v.mapResponse(ctx, response)
}

func (v *lowLevelTxn[R]) addPart(ctx context.Context, part txnPart[R]) error {
	opIndex := v.opCounter[part.Type]
	v.opCounter[part.Type]++

	// Add if
	if part.Type == txnOpIf {
		v.ifs = append(v.ifs, part.If)
		return nil
	}

	// Create low-level operation
	lowLevel, err := part.Factory.Op(ctx)
	if err != nil {
		return errors.PrefixErrorf(err, "cannot create operation [%s][%d]", part.Type, opIndex)
	}
	switch part.Type {
	case txnOpThen:
		if lowLevel.Op.IsTxn() {
			return errors.Errorf(`cannot create operation [then][%d]: operation is a transaction, use Merge or ThenTxn, not Then`, opIndex)
		}
		v.addThen(lowLevel.Op, lowLevel.MapResponse)
	case txnOpThenTxn:
		if !lowLevel.Op.IsTxn() {
			return errors.Errorf(`cannot create operation [thenTxn][%d]: operation is not a transaction, use Then, not ThenTxn`, opIndex)
		}
		v.addThen(lowLevel.Op, lowLevel.MapResponse)
	case txnOpElse:
		v.addElse(lowLevel.Op, lowLevel.MapResponse)
	case txnOpMerge:
		// If it is not a transaction, process it as Then
		if !lowLevel.Op.IsTxn() {
			v.addThen(lowLevel.Op, lowLevel.MapResponse)
			return nil
		}

		// Get transaction parts
		ifs, thenOps, elseOps := lowLevel.Op.Txn()

		// Merge IFs
		v.ifs = append(v.ifs, ifs...)

		// Merge THEN operations
		// The THEN branch will be applied, if all conditions (from all sub-transactions) are met.
		thenStart := len(v.thenOps)
		thenEnd := thenStart + len(thenOps)
		for _, item := range thenOps {
			v.addThen(item, nil)
		}

		// Merge ELSE operations
		// The ELSE branch will be applied only if the conditions of the sub-transaction are not met
		elsePos := -1
		if len(elseOps) > 0 || len(ifs) > 0 {
			elsePos = v.addElse(etcd.OpTxn(ifs, []etcd.Op{}, elseOps), nil)
		}

		// There may be a situation where neither THEN nor ELSE branch is executed:
		// It occurs, when the root transaction fails, but the reason is not in this sub-transaction.

		// On result, compose and map response that corresponds to the original sub-transaction
		// Processor from nested transactions must be invoked first.
		v.processors = append(v.processors, func(ctx context.Context, r *TxnResult[R]) {
			// Get sub-transaction response
			var subTxnResponse *etcd.TxnResponse
			switch {
			case r.succeeded:
				subTxnResponse = &etcd.TxnResponse{
					// The entire transaction succeeded, which means that a partial transaction succeeded as well
					Succeeded: true,
					// Compose responses that corresponds to the original sub-transaction
					Responses: r.Response().Txn().Responses[thenStart:thenEnd],
				}
			case elsePos >= 0:
				subTxnResponse = (*etcd.TxnResponse)(r.Response().Txn().Responses[elsePos].GetResponseTxn())
				if subTxnResponse.Succeeded {
					// Skip mapper bellow, the transaction failed, but not due to a condition in the sub-transaction
					r.AddSubResult(NoResult{})
					return
				}
			default:
				// Skip mapper bellow, the transaction failed, but there is no condition in the sub-transaction
				r.AddSubResult(NoResult{})
				return
			}

			// Call original mapper of the sub transaction
			if subResult, err := lowLevel.MapResponse(ctx, r.Response().SubResponse(subTxnResponse.OpResponse())); err == nil {
				r.AddSubResult(subResult)
			} else {
				r.AddSubResult(err).AddErr(err)
			}
		})
	default:
		panic(errors.Errorf(`unexpected operation type "%s"`, part.Type))
	}

	return nil
}

func (v *lowLevelTxn[R]) addThen(op etcd.Op, mapper MapFn) int {
	v.thenOps = append(v.thenOps, op)

	// If the transaction is successful (then branch), the OP result will be available in the Responses[INDEX],
	// so we can call original mapper with the sub-response.
	index := len(v.thenOps) - 1
	if mapper != nil {
		v.processors = append(v.processors, func(ctx context.Context, r *TxnResult[R]) {
			if r.Succeeded() {
				rawSubResponse := r.Response().Txn().Responses[index]
				subResponse := r.Response().SubResponse(mapRawResponse(rawSubResponse))
				if subResult, err := mapper(ctx, subResponse); err == nil {
					r.AddSubResult(subResult)
				} else {
					r.AddErr(err)
				}
			}
		})
	}

	return index
}

func (v *lowLevelTxn[R]) addElse(op etcd.Op, mapper MapFn) int {
	v.elseOps = append(v.elseOps, op)

	// If the transaction is NOT successful (else branch), the OP result will be available in the Responses[INDEX],
	// so we can call original mapper with the sub-response.
	index := len(v.elseOps) - 1
	if mapper != nil {
		v.processors = append(v.processors, func(ctx context.Context, r *TxnResult[R]) {
			if !r.Succeeded() {
				rawSubResponse := r.Response().Txn().Responses[index]
				subResponse := r.Response().SubResponse(mapRawResponse(rawSubResponse))
				if subResult, err := mapper(ctx, subResponse); err == nil {
					r.AddSubResult(subResult)
				} else {
					r.AddErr(err)
				}
			}
		})
	}

	return index
}

func (v *lowLevelTxn[R]) mapResponse(ctx context.Context, raw RawResponse) *TxnResult[R] {
	// Map transaction response
	r := newTxnResult(&raw, v.result)
	r.succeeded = raw.Txn().Succeeded

	// Use processors
	for _, p := range v.processors {
		p(ctx, r)
	}

	return r
}
