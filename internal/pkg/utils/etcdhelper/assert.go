//nolint:forbidigo // no virtual fs

package etcdhelper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/umisama/go-regexpcache"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type AssertOption func(*assertConfig)

type assertConfig struct {
	ignoredKeyPatterns    []string
	expectedStateFromFile string
}

func WithIgnoredKeyPattern(v string) AssertOption {
	return func(c *assertConfig) {
		c.ignoredKeyPatterns = append(c.ignoredKeyPatterns, v)
	}
}

func withExpectedStateFromFile(path string) AssertOption {
	return func(c *assertConfig) {
		c.expectedStateFromFile = path
	}
}

type tHelper interface {
	Helper()
}

// AssertKeys dumps all keys from an etcd database and compares them with the expected keys.
func AssertKeys(t assert.TestingT, client etcd.KV, expectedKeys []string, ops ...AssertOption) bool {
	if h, ok := t.(tHelper); ok {
		h.Helper()
	}

	// Process options
	c := assertConfig{}
	for _, o := range ops {
		o(&c)
	}

	// Dump actual state
	actualKeysRaw, err := DumpAllKeys(context.Background(), client)
	if err != nil {
		t.Errorf(`cannot dump etcd keys: %s`, err)
		return false
	}

	// Filter out ignored keys
	var actualKeys []string
	for _, key := range actualKeysRaw {
		ignored := false
		for _, pattern := range c.ignoredKeyPatterns {
			if regexpcache.MustCompile(pattern).MatchString(key) {
				ignored = true
				break
			}
		}
		if !ignored {
			actualKeys = append(actualKeys, key)
		}
	}

	// Compare expected and actual keys
	sort.Strings(expectedKeys)
	sort.Strings(actualKeys)

	// Compare expected and actual KVs
	matchedExpected := make(map[int]bool)
	matchedActual := make(map[int]bool)
	for e, expected := range expectedKeys {
		for a, actual := range actualKeys {
			if wildcards.Compare(expected, actual) == nil {
				matchedExpected[e] = true
				matchedActual[a] = true
			}
		}
	}

	var unmatchedExpected []string
	for e, expected := range expectedKeys {
		if !matchedExpected[e] {
			unmatchedExpected = append(unmatchedExpected, fmt.Sprintf(`[%03d] %s`, e, expected))
		}
	}
	if len(unmatchedExpected) > 0 {
		msg := fmt.Sprintf("These keys are in expected but not actual ectd state:\n%s\n", strings.Join(unmatchedExpected, "\n"))
		if c.expectedStateFromFile != "" {
			msg += fmt.Sprintf("Please, update the file:\n\"%s\"", c.expectedStateFromFile)
		}
		assert.Fail(t, msg)
	}

	var unmatchedActual []string
	for a, actual := range actualKeys {
		if !matchedActual[a] {
			unmatchedActual = append(unmatchedActual, fmt.Sprintf(`[%03d] %s`, a, actual))
		}
	}

	if len(unmatchedActual) > 0 {
		msg := fmt.Sprintf("These keys are in actual but not expected ectd state:\n%s\n", strings.Join(unmatchedActual, "\n"))
		if c.expectedStateFromFile != "" {
			msg += fmt.Sprintf("Please, update the file:\n\"%s\"", c.expectedStateFromFile)
		}
		assert.Fail(t, msg)
	}

	return len(unmatchedExpected) == 0 && len(unmatchedActual) == 0
}

// AssertKVsFromFile dumps all KVs from an etcd database and compares them with content of the file.
// In the file, a wildcards can be used, see the wildcards package.
func AssertKVsFromFile(t assert.TestingT, client etcd.KV, path string, ops ...AssertOption) bool {
	data, err := os.ReadFile(path) //nolint:forbidigo // no virtual FS
	if assert.NoError(t, err) || errors.Is(err, os.ErrNotExist) {
		expected := string(data)
		ops = append(ops, withExpectedStateFromFile(path))
		return AssertKVsString(t, client, expected, ops...)
	}
	return false
}

// AssertKVsString dumps all KVs from an etcd database and compares them with the expected string.
// In the expected string, a wildcards can be used, see the wildcards package.
func AssertKVsString(t assert.TestingT, client etcd.KV, expected string, ops ...AssertOption) bool {
	if h, ok := t.(tHelper); ok {
		h.Helper()
	}

	return AssertKVs(t, client, ParseDump(expected), ops...)
}

// AssertKVs dumps all KVs from an etcd database and compares them with the expected KVs.
// In the expected key/value string, a wildcards can be used, see the wildcards package.
func AssertKVs(t assert.TestingT, client etcd.KV, expectedKVs []KV, ops ...AssertOption) bool {
	if h, ok := t.(tHelper); ok {
		h.Helper()
	}

	// Process options
	c := assertConfig{}
	for _, o := range ops {
		o(&c)
	}

	// Dump actual state
	actualKVsRaw, err := DumpAll(context.Background(), client)
	if err != nil {
		t.Errorf(`cannot dump etcd KVs: %s`, err)
		return false
	}

	// Filter out ignored keys
	var actualKVs []KV
	for _, kv := range actualKVsRaw {
		ignored := false
		for _, pattern := range c.ignoredKeyPatterns {
			if regexpcache.MustCompile(pattern).MatchString(kv.Key) {
				ignored = true
				break
			}
		}
		if !ignored {
			actualKVs = append(actualKVs, kv)
		}
	}

	// Dump actual state
	if c.expectedStateFromFile != "" {
		outDir := filepath.Join(filepath.Dir(c.expectedStateFromFile), ".out")              //nolint:forbidigo // no virtual FS
		filePath := filepath.Join(outDir, filepath.Base(c.expectedStateFromFile)+".actual") //nolint:forbidigo // no virtual FS
		assert.NoError(t, os.MkdirAll(outDir, 0o755))                                       //nolint:forbidigo // no virtual FS
		assert.NoError(t, os.WriteFile(filePath, []byte(KVsToString(actualKVs)), 0o644))    //nolint:forbidigo // no virtual FS
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
					msg := fmt.Sprintf("Value of the actual key\n\"%s\"\ndoesn't match the expected key\n\"%s\"", actual.Key, expected.Key)
					if c.expectedStateFromFile != "" {
						msg += fmt.Sprintf("\ndefined in the file\n\"%s\"", c.expectedStateFromFile)
					}
					msg += fmt.Sprintf("\n%s", err)
					assert.Fail(t, msg)
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
		msg := fmt.Sprintf("These keys are in expected but not actual ectd state:\n%s\n", strings.Join(unmatchedExpected, "\n"))
		if c.expectedStateFromFile != "" {
			msg += fmt.Sprintf("Please, update the file:\n\"%s\"", c.expectedStateFromFile)
		}
		assert.Fail(t, msg)
	}

	var unmatchedActual []string
	for a, actual := range actualKVs {
		if !matchedActual[a] {
			unmatchedActual = append(unmatchedActual, fmt.Sprintf(`[%03d] %s`, a, actual.Key))
		}
	}
	if len(unmatchedActual) > 0 {
		msg := fmt.Sprintf("These keys are in actual but not expected ectd state:\n%s\n", strings.Join(unmatchedActual, "\n"))
		if c.expectedStateFromFile != "" {
			msg += fmt.Sprintf("Please, update the file:\n\"%s\"", c.expectedStateFromFile)
		}
		assert.Fail(t, msg)
	}

	return len(unmatchedExpected) == 0 && len(unmatchedActual) == 0
}

// ExpectModificationInPrefix waits until the operation makes some change in etcd or a timeout occurs.
func ExpectModificationInPrefix(t *testing.T, client *etcd.Client, pfx string, operation func()) *etcdserverpb.ResponseHeader {
	t.Helper()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(errors.New("expectation cancelled"))

	ch := client.Watch(ctx, pfx, etcd.WithPrefix(), etcd.WithCreatedNotify())

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

// ExpectModification waits until the operation makes some change in etcd or a timeout occurs.
func ExpectModification(t *testing.T, client *etcd.Client, operation func()) *etcdserverpb.ResponseHeader {
	t.Helper()
	return ExpectModificationInPrefix(t, client, "", operation)
}
