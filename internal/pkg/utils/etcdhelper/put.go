package etcdhelper

import (
	"context"
	"strings"

	"github.com/umisama/go-regexpcache"
	etcd "go.etcd.io/etcd/client/v3"
)

func PutAllFromSnapshot(ctx context.Context, client etcd.KV, dump string) error {
	matches := regexpcache.MustCompile(`(?msU)^<<<<<\n(.+)\n-----\n(.+)\n>>>>>`).FindAllStringSubmatch(dump, -1)
	for _, m := range matches {
		_, err := client.Put(ctx, strings.Trim(m[1], "\r\n\t "), m[2])
		if err != nil {
			return err
		}
	}
	return nil
}
