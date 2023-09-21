package repository

import (
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// cleanupSumKey contains sum of statistics of all old slices deleted by cleanup.
	cleanupSumKey = "_cleanup_sum"
)

type prefix = PrefixT[Value]

type schemaRoot struct {
	prefix
}

type schemaInCategory struct {
	prefix
	category Category
}

type schemaInExport struct {
	prefix
	category Category
}

type schemaInSlice struct {
	prefix
	category Category
}

func newSchema(s *serde.Serde) schemaRoot {
	return schemaRoot{prefix: NewTypedPrefix[Value]("stats", s)}
}

func (s schemaRoot) InCategory(category Category) schemaInCategory {
	return schemaInCategory{prefix: s.prefix.Add(category.String())}
}

func (v schemaInCategory) InObject(objectKey fmt.Stringer) PrefixT[Value] {
	return v.prefix.Add(objectKey.String()).PrefixT()
}

func (v schemaInCategory) InProject(projectID keboola.ProjectID) PrefixT[Value] {
	return v.InObject(projectID)
}

func (v schemaInCategory) InReceiver(k storeKey.ReceiverKey) PrefixT[Value] {
	return v.InObject(k)
}

func (v schemaInCategory) InExport(k storeKey.ExportKey) schemaInExport {
	return schemaInExport{category: v.category, prefix: v.prefix.Add(k.String())}
}

func (v schemaInCategory) InFile(k storeKey.FileKey) PrefixT[Value] {
	return v.InObject(k)
}

func (v schemaInCategory) InSlice(k storeKey.SliceKey) schemaInSlice {
	return schemaInSlice{category: v.category, prefix: v.prefix.Add(k.String())}
}

func (v schemaInExport) CleanupSum() KeyT[Value] {
	return v.prefix.Key(cleanupSumKey)
}

func (v schemaInSlice) PerNode(nodeID string) KeyT[Value] {
	if nodeID == "" {
		panic(errors.New("node ID cannot be empty"))
	}
	return v.prefix.Key(nodeID)
}

func (v schemaInSlice) NodesSum() KeyT[Value] {
	return v.prefix.Key(nodesSumKey)
}
