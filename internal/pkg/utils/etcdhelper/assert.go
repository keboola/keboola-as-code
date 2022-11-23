package etcdhelper

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	etcd "go.etcd.io/etcd/client/v3"
)

// AssertKVs dumps all KVs from an etcd database and compares them with the expected string.
// In the expected string, a wildcards can be used, see the wildcards package.
func AssertKVs(t *testing.T, client etcd.KV, expected string) {
	dump, err := DumpAll(context.Background(), client)
	if err != nil {
		t.Fatalf(`cannot dump etcd KVs: %s`, err)
	}
	wildcards.Assert(t, strings.TrimSpace(expected), strings.TrimSpace(dump), `unexpected etcd state`)
}
