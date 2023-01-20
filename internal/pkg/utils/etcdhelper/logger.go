package etcdhelper

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	. "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type kvWrapper struct {
	KV
	out io.Writer
	id  uint64 // ID generator
}

type txnWrapper struct {
	Txn
	ifOps   []Cmp
	thenOps []Op
	elseOps []Op
	*kvWrapper
}

func KVLogWrapper(kv KV, out io.Writer) KV {
	return &kvWrapper{KV: kv, out: out, id: 1}
}

func (v *kvWrapper) log(requestID uint64, msg string) {
	_, _ = fmt.Fprintf(v.out, "ETCD_REQUEST[%04d] %s\n", requestID, msg)
}

func (v *kvWrapper) nextRequestID() uint64 {
	return atomic.AddUint64(&v.id, 1)
}

func (v *kvWrapper) Put(ctx context.Context, key, val string, opts ...OpOption) (*PutResponse, error) {
	r, err := v.Do(ctx, OpPut(key, val, opts...))
	return r.Put(), err
}

func (v *kvWrapper) Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error) {
	r, err := v.Do(ctx, OpGet(key, opts...))
	return r.Get(), err
}

func (v *kvWrapper) Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error) {
	r, err := v.Do(ctx, OpDelete(key, opts...))
	return r.Del(), err
}

func (v *kvWrapper) Do(ctx context.Context, op Op) (OpResponse, error) {
	requestID := v.nextRequestID()
	startTime := time.Now()
	v.log(requestID, v.startOp(op))
	r, err := v.KV.Do(ctx, op)
	v.log(requestID, v.endOp(op, startTime, r, err))
	return r, err
}

func (v *kvWrapper) startOp(op Op) string {
	var value string
	if op.IsPut() {
		value = string(op.ValueBytes())
	}
	return v.start(&op, opToStr(op), keyToStr(op.KeyBytes(), op.RangeBytes()), value)
}

func (v *kvWrapper) start(op *Op, opName, key, value string) string {
	if key != "" {
		var out strings.Builder
		out.WriteString(fmt.Sprintf(`➡️  %s %s`, opName, key))
		if op.Rev() > 0 {
			out.WriteString(" | rev: ")
			out.WriteString(strconv.FormatInt(op.Rev(), 10))
		}
		if value != "" {
			out.WriteString(" | value:")
			out.WriteString("\n")
			out.WriteString(">>> ")
			out.WriteString(value)
		}
		return out.String()
	}

	// Dump transaction
	var dumpStr string
	if op != nil && op.IsTxn() {
		var dump strings.Builder
		cmpOps, thenOps, elseOps := op.Txn()

		if len(cmpOps) > 0 {
			dump.WriteString("  ➡️  IF:\n")
			for i, item := range cmpOps {
				var expectedResult string
				switch v := item.TargetUnion.(type) {
				case *etcdserverpb.Compare_Version:
					expectedResult = fmt.Sprintf(`%v`, v.Version)
				case *etcdserverpb.Compare_CreateRevision:
					expectedResult = fmt.Sprintf(`%v`, v.CreateRevision)
				case *etcdserverpb.Compare_ModRevision:
					expectedResult = fmt.Sprintf(`%v`, v.ModRevision)
				case *etcdserverpb.Compare_Value:
					expectedResult = fmt.Sprintf(`"%s"`, string(v.Value))
				case *etcdserverpb.Compare_Lease:
					expectedResult = fmt.Sprintf(`%v`, v.Lease)
				default:
					panic(errors.Errorf(`unexpected type "%T"`, item.TargetUnion))
				}
				dump.WriteString(fmt.Sprintf("  %03d %s %s %v %s\n", i+1, keyToStr(item.Key, item.RangeEnd), item.Target, item.Result, expectedResult))
			}
		}

		if len(thenOps) > 0 {
			dump.WriteString("  ➡️  THEN:\n")
			for i, item := range thenOps {
				linePrefix := fmt.Sprintf("  %03d ", i+1)
				prefixLines(linePrefix, v.startOp(item), &dump)
			}
		}

		if len(elseOps) > 0 {
			dump.WriteString("  ➡️  ELSE:\n")
			for i, item := range elseOps {
				linePrefix := fmt.Sprintf("  %03d ", i+1)
				prefixLines(linePrefix, v.startOp(item), &dump)
			}
		}

		dumpStr = dump.String()
	}

	if dumpStr == "" {
		return fmt.Sprintf("➡️  %s", opName)
	} else {
		return fmt.Sprintf("➡️  %s\n%s", opName, dumpStr)
	}
}

func (v *kvWrapper) endOp(op Op, startTime time.Time, r OpResponse, err error) string {
	return v.end(opToStr(op), keyToStr(op.KeyBytes(), op.RangeBytes()), startTime, r, err)
}

func (v *kvWrapper) end(op, key string, startTime time.Time, r OpResponse, err error) string {
	if err != nil {
		if key == "" {
			return fmt.Sprintf(`✖  %s | error | %s | %s`, op, err, time.Since(startTime))
		} else {
			return fmt.Sprintf(`✖  %s %s | error | %s | %s`, op, key, err, time.Since(startTime))
		}
	} else if get := r.Get(); get != nil {
		return fmt.Sprintf(`✔️️  %s %s | rev: %v | count: %d | %s`, op, key, get.Header.Revision, get.Count, time.Since(startTime))
	} else if put := r.Put(); put != nil {
		return fmt.Sprintf("✔️️  %s %s | rev: %v | %s", op, key, put.Header.Revision, time.Since(startTime))
	} else if del := r.Del(); del != nil {
		return fmt.Sprintf(`✔️️  %s %s | rev: %v | deleted: %d| %s`, op, key, del.Header.Revision, del.Deleted, time.Since(startTime))
	} else if txn := r.Txn(); txn != nil {
		return fmt.Sprintf(`✔️️  %s | succeeded: %t | rev: %v | %s`, op, txn.Succeeded, txn.Header.Revision, time.Since(startTime))
	} else {
		if key == "" {
			return fmt.Sprintf(`✔️️  %s | %s`, op, time.Since(startTime))
		} else {
			return fmt.Sprintf(`✔️️  %s %s | %s`, op, key, time.Since(startTime))
		}
	}
}

func (v *kvWrapper) Txn(ctx context.Context) Txn {
	return &txnWrapper{Txn: v.KV.Txn(ctx), kvWrapper: v}
}

func (v *txnWrapper) If(ops ...Cmp) Txn {
	v.Txn.If(ops...)
	v.ifOps = append(v.ifOps, ops...)
	return v
}

func (v *txnWrapper) Then(ops ...Op) Txn {
	v.Txn.Then(ops...)
	v.thenOps = append(v.thenOps, ops...)
	return v
}

func (v *txnWrapper) Else(ops ...Op) Txn {
	v.Txn.Else(ops...)
	v.elseOps = append(v.elseOps, ops...)
	return v
}

func (v *txnWrapper) Commit() (*TxnResponse, error) {
	requestID := v.nextRequestID()
	startTime := time.Now()
	op := OpTxn(v.ifOps, v.thenOps, v.elseOps)
	v.log(requestID, v.start(&op, "TXN", "", ""))
	r, err := v.Txn.Commit()
	v.log(requestID, v.end("TXN", "", startTime, r.OpResponse(), err))
	return r, err
}

func quote(v string) string {
	if v == "" {
		return ""
	}
	return fmt.Sprintf(`"%s"`, v)
}

func keyToStr(key, end []byte) string {
	if len(key) == 0 {
		return ""
	} else if len(end) > 0 {
		return fmt.Sprintf(`["%s", "%s")`, string(key), string(end))
	} else {
		return quote(string(key))
	}
}

func opToStr(op Op) string {
	switch {
	case op.IsGet():
		return "GET"
	case op.IsPut():
		return "PUT"
	case op.IsDelete():
		return "DEL"
	case op.IsTxn():
		return "TXN"
	}
	return "n/a"
}

func prefixLines(prefix, lines string, out *strings.Builder) {
	s := bufio.NewScanner(strings.NewReader(lines))
	for s.Scan() {
		out.WriteString(prefix)
		out.WriteString(s.Text())
		out.WriteString("\n")
	}
}
