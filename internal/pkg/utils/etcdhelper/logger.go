package etcdhelper

import (
	"context"
	"fmt"
	"io"
	"time"

	. "go.etcd.io/etcd/client/v3"
)

type kvWrapper struct {
	KV
	out io.Writer
}

type txnWrapper struct {
	Txn
	*kvWrapper
}

func KVLogWrapper(kv KV, out io.Writer) KV {
	return &kvWrapper{KV: kv, out: out}
}

func (v *kvWrapper) log(format string, a ...any) {
	format = "etcd-client " + format + "\n"
	_, _ = fmt.Fprintf(v.out, format, a...)
}

func (v *kvWrapper) Put(ctx context.Context, key, val string, opts ...OpOption) (*PutResponse, error) {
	startTime := time.Now()
	v.logStart("PUT", key)
	r, err := v.KV.Put(ctx, key, val, opts...)
	v.logEnd("PUT", key, startTime, r.OpResponse(), err)
	return r, err
}

func (v *kvWrapper) Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error) {
	startTime := time.Now()
	v.logStart("GET", key)
	r, err := v.KV.Get(ctx, key, opts...)
	v.logEnd("GET", key, startTime, r.OpResponse(), err)
	return r, err
}

func (v *kvWrapper) Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error) {
	startTime := time.Now()
	v.logStart("DEL", key)
	r, err := v.KV.Delete(ctx, key, opts...)
	v.logEnd("DEL", key, startTime, r.OpResponse(), err)
	return r, err
}

func (v *kvWrapper) Do(ctx context.Context, op Op) (OpResponse, error) {
	key := string(op.KeyBytes())
	var opName string
	switch {
	case op.IsGet():
		opName = "GET"
	case op.IsPut():
		opName = "PUT"
	case op.IsDelete():
		opName = "DEL"
	case op.IsTxn():
		opName = "TXN"
	}

	startTime := time.Now()
	v.logStart(opName, key)

	r, err := v.KV.Do(ctx, op)
	v.logEnd(opName, key, startTime, r, err)
	return r, err
}

func (v *kvWrapper) logStart(op, key string) {
	v.log(`%s "%s" | start`, op, key)
}

func (v *kvWrapper) logEnd(op, key string, startTime time.Time, r OpResponse, err error) {
	if err != nil {
		v.log(`%s "%s" | error | %s | %s`, op, key, err, time.Since(startTime))
	} else if r.Get() != nil {
		v.log(`%s "%s" | done | count: %d | %s`, op, key, r.Get().Count, time.Since(startTime))
	} else if r.Del() != nil {
		v.log(`%s "%s" | done | deleted: %d| %s`, op, key, r.Del().Deleted, time.Since(startTime))
	} else {
		v.log(`%s "%s" | done | %s`, op, key, time.Since(startTime))
	}
}

func (v *kvWrapper) Txn(ctx context.Context) Txn {
	return &txnWrapper{Txn: v.KV.Txn(ctx), kvWrapper: v}
}

func (v *txnWrapper) Commit() (*TxnResponse, error) {
	startTime := time.Now()
	v.log(`TXN | start`)

	r, err := v.Txn.Commit()
	if err != nil {
		v.log(`TXN | error | %s | %s`, err, time.Since(startTime))
	} else {
		v.log(`TXN | done | succeeded: %t | %s`, r.Succeeded, time.Since(startTime))
	}

	return r, err
}
