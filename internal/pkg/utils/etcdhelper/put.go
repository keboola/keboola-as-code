package etcdhelper

import (
	"context"
	"strings"

	"github.com/umisama/go-regexpcache"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type KV struct {
	Key   string
	Value string
	// Lease contains etcd lease number.
	// In parsed expectations: 1 means a lease is expected, 0 means no lease is expected.
	Lease int64
}

func PutAllFromSnapshot(ctx context.Context, client *etcd.Client, dump string) (err error) {
	var session *concurrency.Session
	for _, kv := range ParseDump(dump) {
		// Create etcd session and use the lease ID, if required
		var lease etcd.LeaseID
		if kv.Lease > 0 {
			if session == nil {
				session, err = concurrency.NewSession(client)
				if err != nil {
					return err
				}
			}
			lease = session.Lease()
		}

		_, err := client.Put(ctx, kv.Key, kv.Value, etcd.WithLease(lease))
		if err != nil {
			return err
		}
	}
	return nil
}

func ParseDump(dump string) (out []KV) {
	matches := regexpcache.MustCompile(`(?msU)^<<<<<\n(.+)( \(lease\))?\n-----\n(.+)\n>>>>>`).FindAllStringSubmatch(dump, -1)
	for _, m := range matches {
		// Lease is a random integer, something like session ID.
		// In parsed expectations: 1 means a lease is expected, 0 means no lease is expected.
		lease := int64(0)
		if m[2] != "" {
			lease = 1
		}

		out = append(out, KV{Key: strings.Trim(m[1], "\r\n\t "), Value: m[3], Lease: lease})
	}
	return out
}
