package etcdhelper

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"
)

func AssertKVs(t *testing.T, client etcd.KV, expected string) {
	dump, err := DumpAll(context.Background(), client)
	if err != nil {
		t.Fatalf(`cannot dump etcd KVs: %s`, err)
	}
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(dump), `unexpected etcd state`)
}
