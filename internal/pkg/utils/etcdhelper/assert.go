package etcdhelper

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"
)

// AssertKVsString dumps all KVs from an etcd database and compares them with the expected string.
// In the expected string, a wildcards can be used, see the wildcards package.
func AssertKVsString(t assert.TestingT, client etcd.KV, expected string) {
	AssertKVs(t, client, ParseDump(expected))
}

// AssertKVs dumps all KVs from an etcd database and compares them with the expected KVs.
// In the expected key/value string, a wildcards can be used, see the wildcards package.
func AssertKVs(t assert.TestingT, client etcd.KV, expectedKVs []KV) {
	actualKVs, err := DumpAll(context.Background(), client)
	if err != nil {
		t.Errorf(`cannot dump etcd KVs: %s`, err)
		return
	}

	// Compare expected and actual KVs
	matchedExpected := make(map[int]bool)
	matchedActual := make(map[int]bool)
	for e, expected := range expectedKVs {
		for a, actual := range actualKVs {
			// Each actual key can be used only once
			if matchedActual[a] {
				continue
			}

			if wildcards.Compare(expected.Key, actual.Key) == nil {
				matchedExpected[e] = true
				matchedActual[a] = true
				if err := wildcards.Compare(expected.Value, actual.Value); err == nil {
					// Value matched, check lease presence.
					if expected.Lease == 1 && actual.Lease == 0 {
						assert.Fail(t, fmt.Sprintf(`The key "%s" is not supposed to have a lease, but it was found.`, actual.Key))
					} else if expected.Lease == 0 && actual.Lease > 0 {
						assert.Fail(t, fmt.Sprintf(`The key "%s" is supposed to have lease, but it was not found.`, actual.Key))
					}
					break
				} else {
					assert.Fail(t, fmt.Sprintf("Value of the actual key\n\"%s\"\ndoesn't match to the expected key\n\"%s\":\n%s", actual.Key, expected.Key, err))
				}
			}
		}
	}

	var unmatchedExpected []string
	for e, expected := range expectedKVs {
		if !matchedExpected[e] {
			unmatchedExpected = append(unmatchedExpected, fmt.Sprintf(`[%03d] %s`, e, expected.Key))
		}
	}
	if len(unmatchedExpected) > 0 {
		assert.Fail(t, fmt.Sprintf("These keys are in expected but not actual ectd state:\n%s", strings.Join(unmatchedExpected, "\n")))
	}

	var unmatchedActual []string
	for a, actual := range actualKVs {
		if !matchedActual[a] {
			unmatchedActual = append(unmatchedActual, fmt.Sprintf(`[%03d] %s`, a, actual.Key))
		}
	}
	if len(unmatchedActual) > 0 {
		assert.Fail(t, fmt.Sprintf("These keys are in actual but not expected ectd state:\n%s", strings.Join(unmatchedActual, "\n")))
	}
}

// ExpectModification waits until the operation makes some change in etcd or a timeout occurs.
func ExpectModification(t *testing.T, client *etcd.Client, operation func()) *etcdserverpb.ResponseHeader {
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
		return &resp.Header
	}

	return nil
}
