package etcdhelper

import (
	"bytes"
	"context"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
)

func KVsToString(kvs []KV) string {
	var b strings.Builder
	for i, kv := range kvs {
		// Start
		if i != 0 {
			b.WriteString("\n")
		}
		b.WriteString("<<<<<\n")

		// Dump key
		b.WriteString(kv.Key)
		if kv.Lease > 0 {
			b.WriteString(" (lease)")
		}
		b.WriteByte('\n')

		// Separator
		b.WriteString("-----\n")

		// Dump value
		val := kv.Value
		b.WriteString(val)
		if len(val) == 0 || val[len(val)-1] != '\n' {
			b.WriteByte('\n')
		}

		// End
		b.WriteString(">>>>>\n")
	}

	return b.String()
}

func DumpAllToString(ctx context.Context, client etcd.KV) (string, error) {
	_, err := client.Get(ctx, "", etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
	if err != nil {
		return "", err
	}

	kvs, err := DumpAll(ctx, client)
	if err != nil {
		return "", err
	}

	return KVsToString(kvs), nil
}

func DumpAllKeys(ctx context.Context, client etcd.KV) (keys []string, err error) {
	r, err := client.Get(ctx, "", etcd.WithPrefix(), etcd.WithKeysOnly(), etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
	if err != nil {
		return nil, err
	}
	for _, kv := range r.Kvs {
		keys = append(keys, string(kv.Key))
	}
	return keys, nil
}

func DumpAll(ctx context.Context, client etcd.KV) (out []KV, err error) {
	r, err := client.Get(ctx, "", etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
	if err != nil {
		return nil, err
	}

	for _, kv := range r.Kvs {
		// Try format value as a JSON
		val := kv.Value
		if bytes.HasPrefix(val, []byte{'{'}) {
			target := orderedmap.New()
			if err := json.Decode(val, target); err == nil {
				val = json.MustEncode(target, true)
			}
		}

		out = append(out, KV{Key: string(kv.Key), Value: string(val), Lease: kv.Lease})
	}
	return out, nil
}
