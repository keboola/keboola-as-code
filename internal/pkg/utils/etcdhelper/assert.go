package etcdhelper

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"
)

// AssertKVs dumps all KVs from an etcd database and compares them with the expected string.
// In the expected string, a wildcards can be used, see the wildcards package.
func AssertKVs(t *testing.T, client etcd.KV, expected string) {
	t.Helper()
	dump, err := DumpAll(context.Background(), client)
	if err != nil {
		t.Fatalf(`cannot dump etcd KVs: %s`, err)
	}
	wildcards.Assert(t, strings.TrimSpace(expected), strings.TrimSpace(dump), `unexpected etcd state`)
}

// ExpectModification waits until the operation makes some change in etcd or a timeout occurs.
func ExpectModification(t *testing.T, client *etcd.Client, operation func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := client.Watch(ctx, "", etcd.WithPrefix(), etcd.WithCreatedNotify())

	resp := <-ch
	assert.True(t, resp.Created)

	operation()

	select {
	case <-ctx.Done():
		t.Fatal("context cancelled when waiting for an etcd modification")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout when waiting for an etcd modification")
	case resp = <-ch:
		if resp.Err() != nil {
			t.Fatal(resp.Err())
		}
		return
	}

}
