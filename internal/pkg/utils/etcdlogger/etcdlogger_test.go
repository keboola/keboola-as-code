package etcdlogger_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func TestKVLogWrapper_DefaultConfig(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	runEtcdOperations(t, &out)

	wildcards.Assert(t, `
ETCD_REQUEST[0002] ➡️  PUT "key1" | value:
>>> value1

ETCD_REQUEST[0002] ✔️  PUT "key1" | rev: %s | duration: %s

ETCD_REQUEST[0003] ➡️  PUT "key2" | value:
>>> value2

ETCD_REQUEST[0003] ✔️  PUT "key2" | rev: %s | duration: %s

ETCD_REQUEST[0004] ➡️  GET ["key", "kez")

ETCD_REQUEST[0004] ✔️  GET ["key", "kez") | rev: %s | count: 2 | loaded: 1 | duration: %s

ETCD_REQUEST[0005] ➡️  DEL "key2"

ETCD_REQUEST[0005] ✔️  DEL "key2" | rev: %s | count: 1 | duration: %s

ETCD_REQUEST[0006] ➡️  TXN
  ➡️  IF:
  001 "key1" VERSION NOT_EQUAL 0
  ➡️  THEN:
  001 ➡️  DEL "key1"

ETCD_REQUEST[0006] ✔️  TXN | succeeded: true | rev: %s | duration: %s

ETCD_REQUEST[0007] ➡️  TXN
  ➡️  IF:
  001 "key1" VERSION NOT_EQUAL 0
  ➡️  THEN:
  001 ➡️  DEL "key1"

ETCD_REQUEST[0007] ✔️  TXN | succeeded: false | rev: %s | duration: %s

ETCD_REQUEST[0008] ➡️  TXN
  ➡️  THEN:
  001 ➡️  PUT "key1" | value:
  001 >>> value1
  002 ➡️  PUT "key1" | value:
  002 >>> value1

ETCD_REQUEST[0008] ✖  TXN | error: etcdserver: duplicate key given in txn request | duration: %s
`, out.String())
}

func TestKVLogWrapper_MinimalConfig(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	runEtcdOperations(t, &out, etcdlogger.WithMinimal(), etcdlogger.WithNewLineSeparator(false))

	wildcards.Assert(t, `
➡️  PUT "key1"
✔️  PUT "key1"
➡️  PUT "key2"
✔️  PUT "key2"
➡️  GET ["key", "kez")
✔️  GET ["key", "kez") | count: 2 | loaded: 1
➡️  DEL "key2"
✔️  DEL "key2" | count: 1
➡️  TXN
  ➡️  IF:
  001 "key1" VERSION NOT_EQUAL 0
  ➡️  THEN:
  001 ➡️  DEL "key1"
✔️  TXN | succeeded: true
➡️  TXN
  ➡️  IF:
  001 "key1" VERSION NOT_EQUAL 0
  ➡️  THEN:
  001 ➡️  DEL "key1"
✔️  TXN | succeeded: false
➡️  TXN
  ➡️  THEN:
  001 ➡️  PUT "key1"
  002 ➡️  PUT "key1"
✖  TXN | error: etcdserver: duplicate key given in txn request
`, out.String())
}

func runEtcdOperations(t *testing.T, out io.Writer, opts ...etcdlogger.Option) {
	t.Helper()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(errors.New("operations cancelled"))

	client := etcdhelper.ClientForTest(t, etcdhelper.TmpNamespace(t))
	kv := etcdlogger.KVLogWrapper(client, out, opts...)
	session, err := concurrency.NewSession(client)
	require.NoError(t, err)

	_, err = kv.Put(ctx, "key1", "value1")
	require.NoError(t, err)

	_, err = kv.Put(ctx, "key2", "value2", etcd.WithLease(session.Lease()))
	require.NoError(t, err)

	_, err = kv.Get(ctx, "key", etcd.WithPrefix(), etcd.WithLimit(1))
	require.NoError(t, err)

	_, err = kv.Delete(ctx, "key2")
	require.NoError(t, err)

	txn1 := kv.Txn(ctx).
		If(etcd.Compare(etcd.Version("key1"), "!=", 0)).
		Then(etcd.OpDelete("key1"))

	_, err = txn1.Commit()
	require.NoError(t, err)

	_, err = txn1.Commit()
	require.NoError(t, err)

	_, err = kv.Do(ctx, etcd.OpTxn(nil, []etcd.Op{etcd.OpPut("key1", "value1"), etcd.OpPut("key1", "value1")}, nil)) // duplicate key
	assert.Error(t, err)
}
