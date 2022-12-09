package etcdhelper

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
)

func DumpAll(ctx context.Context, client etcd.KV) (string, error) {
	r, err := client.Get(ctx, "", etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
	if err != nil {
		return "", err
	}

	var b strings.Builder

	for i, kv := range r.Kvs {
		// Start
		if i != 0 {
			b.WriteString("\n")
		}
		b.WriteString("<<<<<\n")

		// Dump key
		b.Write(kv.Key)
		if kv.Lease > 0 {
			b.WriteString(fmt.Sprintf(" (lease=%d)", kv.Lease))
		}
		b.WriteByte('\n')

		// Separator
		b.WriteString("-----\n")

		// Try format value as a JSON
		val := kv.Value
		if bytes.HasPrefix(val, []byte{'{'}) {
			target := orderedmap.New()
			if err := json.Decode(val, target); err == nil {
				val = json.MustEncode(target, true)
			}
		}

		// Dump value
		b.Write(val)
		if len(val) == 0 || val[len(val)-1] != '\n' {
			b.WriteByte('\n')
		}

		// End
		b.WriteString(">>>>>\n")
	}

	return b.String(), nil
}
