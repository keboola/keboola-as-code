package etcdhelper

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	. "go.etcd.io/etcd/client/v3"
)

type kvWrapper struct {
	KV
	out io.Writer
	id  uint64 // ID generator
}

type txnWrapper struct {
	Txn
	*kvWrapper
}

func KVLogWrapper(kv KV, out io.Writer) KV {
	return &kvWrapper{KV: kv, out: out, id: 1}
}

func (v *kvWrapper) log(requestID uint64, format string, a ...any) {
	format = "ETCD_REQUEST[%04d] " + format + "\n"
	a = append([]any{requestID}, a...)
	_, _ = fmt.Fprintf(v.out, format, a...)
}

func (v *kvWrapper) nextRequestID() uint64 {
	return atomic.AddUint64(&v.id, 1)
}

func (v *kvWrapper) Put(ctx context.Context, key, val string, opts ...OpOption) (*PutResponse, error) {
	requestID := v.nextRequestID()
	startTime := time.Now()
	v.logStart(requestID, "PUT", key)
	r, err := v.KV.Put(ctx, key, val, opts...)
	v.logEnd(requestID, "PUT", key, val, startTime, r.OpResponse(), err)
	return r, err
}

func (v *kvWrapper) Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error) {
	requestID := v.nextRequestID()
	startTime := time.Now()
	v.logStart(requestID, "GET", key)
	r, err := v.KV.Get(ctx, key, opts...)
	v.logEnd(requestID, "GET", key, "", startTime, r.OpResponse(), err)
	return r, err
}

func (v *kvWrapper) Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error) {
	requestID := v.nextRequestID()
	startTime := time.Now()
	v.logStart(requestID, "DEL", key)
	r, err := v.KV.Delete(ctx, key, opts...)
	v.logEnd(requestID, "DEL", key, "", startTime, r.OpResponse(), err)
	return r, err
}

func (v *kvWrapper) Do(ctx context.Context, op Op) (OpResponse, error) {
	key := string(op.KeyBytes())
	var opName string
	var val string
	switch {
	case op.IsGet():
		opName = "GET"
	case op.IsPut():
		opName = "PUT"
		val = string(op.ValueBytes())
	case op.IsDelete():
		opName = "DEL"
	case op.IsTxn():
		opName = "TXN"
	}

	requestID := v.nextRequestID()
	startTime := time.Now()
	v.logStart(requestID, opName, key)

	r, err := v.KV.Do(ctx, op)
	v.logEnd(requestID, opName, key, val, startTime, r, err)
	return r, err
}

func (v *kvWrapper) logStart(requestID uint64, op, key string) {
	v.log(requestID, `%s "%s" | start`, op, key)
}

func (v *kvWrapper) logEnd(requestID uint64, op, key, val string, startTime time.Time, r OpResponse, err error) {
	if err != nil {
		v.log(requestID, `%s "%s" | error | %s | %s`, op, key, err, time.Since(startTime))
	} else if r.Get() != nil {
		v.log(requestID, `%s "%s" | done | count: %d | %s`, op, key, r.Get().Count, time.Since(startTime))
	} else if r.Put() != nil {
		v.log(requestID, "%s \"%s\" | done | %s | value:\n%s", op, key, time.Since(startTime), val)
	} else if r.Del() != nil {
		v.log(requestID, `%s "%s" | done | deleted: %d| %s`, op, key, r.Del().Deleted, time.Since(startTime))
	} else {
		v.log(requestID, `%s "%s" | done | %s`, op, key, time.Since(startTime))
	}
}

func (v *kvWrapper) Txn(ctx context.Context) Txn {
	return &txnWrapper{Txn: v.KV.Txn(ctx), kvWrapper: v}
}

func (v *txnWrapper) Commit() (*TxnResponse, error) {
	requestID := v.nextRequestID()
	startTime := time.Now()
	v.log(requestID, `TXN | start`)

	r, err := v.Txn.Commit()
	if err != nil {
		v.log(requestID, `TXN | error | %s | %s`, err, time.Since(startTime))
	} else {
		v.log(requestID, `TXN | done | succeeded: %t | %s`, r.Succeeded, time.Since(startTime))
	}

	return r, err
}
