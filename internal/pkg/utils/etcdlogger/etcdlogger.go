package etcdlogger

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type kvWrapper struct {
	etcd.KV
	out    io.Writer
	config config
	id     uint64 // ID generator
}

type txnWrapper struct {
	etcd.Txn
	ifOps   []etcd.Cmp
	thenOps []etcd.Op
	elseOps []etcd.Op
	*kvWrapper
}

type config struct {
	RequestNumber bool
	PutValue      bool
	Revision      bool
	Duration      bool
	NewLineSep    bool
}

type Option func(*config)

func KVLogWrapper(kv etcd.KV, out io.Writer, opts ...Option) etcd.KV {
	return &kvWrapper{KV: kv, out: out, config: newConfig(opts), id: 1}
}

func newConfig(opts []Option) config {
	cfg := config{
		RequestNumber: true,
		PutValue:      true,
		Revision:      true,
		Duration:      true,
		NewLineSep:    true,
	}

	for _, o := range opts {
		o(&cfg)
	}

	return cfg
}

func WithMinimal() Option {
	return func(c *config) {
		WithoutRequestNumber()(c)
		WithoutPutValue()(c)
		WithoutRevision()(c)
		WithoutDuration()(c)
	}
}

func WithoutRequestNumber() Option {
	return func(c *config) {
		c.RequestNumber = false
	}
}

func WithoutPutValue() Option {
	return func(c *config) {
		c.PutValue = false
	}
}

func WithoutRevision() Option {
	return func(c *config) {
		c.Revision = false
	}
}

func WithoutDuration() Option {
	return func(c *config) {
		c.Duration = false
	}
}

func WithNewLineSeparator(v bool) Option {
	return func(c *config) {
		c.NewLineSep = v
	}
}

func (v *kvWrapper) log(requestID uint64, msg string) {
	var out bytes.Buffer

	if v.config.RequestNumber {
		_, _ = fmt.Fprintf(&out, "ETCD_REQUEST[%04d] ", requestID)
	}

	out.WriteString(msg)
	out.WriteString("\n")

	if v.config.NewLineSep {
		out.WriteString("\n")
	}

	_, _ = v.out.Write(out.Bytes())
}

func (v *kvWrapper) nextRequestID() uint64 {
	return atomic.AddUint64(&v.id, 1)
}

func (v *kvWrapper) Put(ctx context.Context, key, val string, opts ...etcd.OpOption) (*etcd.PutResponse, error) {
	r, err := v.Do(ctx, etcd.OpPut(key, val, opts...))
	return r.Put(), err
}

func (v *kvWrapper) Get(ctx context.Context, key string, opts ...etcd.OpOption) (*etcd.GetResponse, error) {
	r, err := v.Do(ctx, etcd.OpGet(key, opts...))
	return r.Get(), err
}

func (v *kvWrapper) Delete(ctx context.Context, key string, opts ...etcd.OpOption) (*etcd.DeleteResponse, error) {
	r, err := v.Do(ctx, etcd.OpDelete(key, opts...))
	return r.Del(), err
}

func (v *kvWrapper) Do(ctx context.Context, op etcd.Op) (etcd.OpResponse, error) {
	requestID := v.nextRequestID()
	startTime := time.Now()
	v.log(requestID, v.startOp(op))
	r, err := v.KV.Do(ctx, op)
	v.log(requestID, v.endOp(op, startTime, r, err))
	return r, err
}

func (v *kvWrapper) startOp(op etcd.Op) string {
	var value string
	if op.IsPut() {
		value = string(op.ValueBytes())
	}
	return v.start(&op, opToStr(op), keyToStr(op.KeyBytes(), op.RangeBytes()), value)
}

// writePair writes the key-value pair details to the output builder.
// It includes metadata like revision, count-only flag, keys-only flag, serializable flag, and the value.
func (v *kvWrapper) writePair(op *etcd.Op, key, value string, out *strings.Builder) {
	out.WriteString(" ")
	out.WriteString(key)
	if op.Rev() > 0 {
		out.WriteString(" | rev")
		if v.config.Revision {
			out.WriteString(": ")
			out.WriteString(strconv.FormatInt(op.Rev(), 10))
		}
	}
	if op.IsCountOnly() {
		out.WriteString(" | count only")
	}
	if op.IsKeysOnly() {
		out.WriteString(" | keys only")
	}
	if op.IsSerializable() {
		out.WriteString(" | serializable")
	}
	if value != "" && v.config.PutValue {
		out.WriteString(" | value:")
		out.WriteString("\n")
		out.WriteString(">>> ")
		out.WriteString(value)
	}
}

func (v *kvWrapper) start(op *etcd.Op, opName, key, value string) string {
	var out strings.Builder

	out.WriteString(fmt.Sprintf("➡️  %s", opName))

	if key != "" {
		v.writePair(op, key, value, &out)
	}

	if op == nil || !op.IsTxn() {
		return out.String()
	}

	// Dump transaction
	cmpOps, thenOps, elseOps := op.Txn()

	if len(cmpOps) > 0 {
		out.WriteString("\n")
		out.WriteString("  ➡️  IF:")
		for i, item := range cmpOps {
			out.WriteString("\n")
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
			out.WriteString(fmt.Sprintf("  %03d %s %s %v %s", i+1, keyToStr(item.Key, item.RangeEnd), item.Target, item.Result, expectedResult))
		}
	}

	if len(thenOps) > 0 {
		out.WriteString("\n")
		out.WriteString("  ➡️  THEN:")
		for i, item := range thenOps {
			out.WriteString("\n")
			linePrefix := fmt.Sprintf("  %03d ", i+1)
			prefixLines(linePrefix, v.startOp(item), &out)
		}
	}

	if len(elseOps) > 0 {
		out.WriteString("\n")
		out.WriteString("  ➡️  ELSE:")
		for i, item := range elseOps {
			out.WriteString("\n")
			linePrefix := fmt.Sprintf("  %03d ", i+1)
			prefixLines(linePrefix, v.startOp(item), &out)
		}
	}

	return out.String()
}

func (v *kvWrapper) endOp(op etcd.Op, startTime time.Time, r etcd.OpResponse, err error) string {
	opStr := opToStr(op)
	keyStr := keyToStr(op.KeyBytes(), op.RangeBytes())
	duration := time.Since(startTime)
	if err != nil {
		return v.logEnd(err, opStr, keyStr, "", 0, -1, -1, duration)
	} else if get := r.Get(); get != nil {
		return v.logEnd(nil, opStr, keyStr, "", get.Header.Revision, get.Count, len(get.Kvs), duration)
	} else if put := r.Put(); put != nil {
		return v.logEnd(nil, opStr, keyStr, "", put.Header.Revision, -1, -1, duration)
	} else if del := r.Del(); del != nil {
		return v.logEnd(nil, opStr, keyStr, "", del.Header.Revision, del.Deleted, -1, duration)
	} else if txn := r.Txn(); txn != nil {
		return v.logEnd(nil, opStr, keyStr, fmt.Sprintf("| succeeded: %t", txn.Succeeded), txn.Header.Revision, -1, -1, duration)
	} else {
		return v.logEnd(nil, opStr, keyStr, "", 0, -1, -1, duration)
	}
}

func (v *kvWrapper) logEnd(err error, op, key, extra string, rev, count int64, loaded int, duration time.Duration) string {
	var out strings.Builder

	// Status
	if err == nil {
		out.WriteString("✔️")
	} else {
		out.WriteString("✖")
	}
	out.WriteString("  ")

	// Operation
	out.WriteString(op)

	// Key
	if key != "" {
		out.WriteString(" ")
		out.WriteString(key)
	}

	// Extra
	if extra != "" {
		out.WriteString(" ")
		out.WriteString(extra)
	}

	if err != nil {
		// Error
		out.WriteString(" | error: ")
		out.WriteString(err.Error())
	} else {
		// Revision
		if rev != 0 && v.config.Revision {
			out.WriteString(" | rev: ")
			out.WriteString(strconv.FormatInt(rev, 10))
		}

		// Count
		if count != -1 {
			out.WriteString(" | count: ")
			out.WriteString(strconv.FormatInt(count, 10))
		}

		// Loaded - for get operation with limit
		if loaded != -1 && count != int64(loaded) {
			out.WriteString(" | loaded: ")
			out.WriteString(strconv.FormatInt(int64(loaded), 10))
		}
	}

	// Duration
	if duration != 0 && v.config.Duration {
		out.WriteString(" | duration: ")
		out.WriteString(duration.String())
	}

	return out.String()
}

func (v *kvWrapper) Txn(ctx context.Context) etcd.Txn {
	return &txnWrapper{Txn: v.KV.Txn(ctx), kvWrapper: v}
}

func (v *txnWrapper) If(ops ...etcd.Cmp) etcd.Txn {
	v.Txn.If(ops...)
	v.ifOps = append(v.ifOps, ops...)
	return v
}

func (v *txnWrapper) Then(ops ...etcd.Op) etcd.Txn {
	v.Txn.Then(ops...)
	v.thenOps = append(v.thenOps, ops...)
	return v
}

func (v *txnWrapper) Else(ops ...etcd.Op) etcd.Txn {
	v.Txn.Else(ops...)
	v.elseOps = append(v.elseOps, ops...)
	return v
}

func (v *txnWrapper) Commit() (*etcd.TxnResponse, error) {
	requestID := v.nextRequestID()
	startTime := time.Now()
	op := etcd.OpTxn(v.ifOps, v.thenOps, v.elseOps)
	v.log(requestID, v.start(&op, "TXN", "", ""))
	r, err := v.Txn.Commit()
	v.log(requestID, v.endOp(op, startTime, r.OpResponse(), err))
	return r, err
}

func quote(v string) string {
	if v == "" {
		return ""
	}
	return fmt.Sprintf(`"%s"`, v)
}

func keyToStr(key, end []byte) string {
	keyStr := strings.ReplaceAll(string(key), "\000", "<NUL>")
	endStr := strings.ReplaceAll(string(end), "\000", "<NUL>")
	switch {
	case len(keyStr) == 0:
		return ""
	case len(endStr) > 0:
		return fmt.Sprintf(`["%s", "%s")`, keyStr, endStr)
	default:
		return quote(keyStr)
	}
}

func opToStr(op etcd.Op) string {
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
	i := 0
	for s.Scan() {
		if i != 0 {
			out.WriteString("\n")
		}
		out.WriteString(prefix)
		out.WriteString(s.Text())
		i++
	}
}
