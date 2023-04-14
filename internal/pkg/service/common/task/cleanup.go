package task

import (
	"context"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (n *Node) isForCleanup(t Task) bool {
	now := n.clock.Now()
	if t.IsProcessing() {
		taskAge := now.Sub(t.CreatedAt.Time())
		if taskAge >= CleanupUnfinishedTasksAfter {
			return true
		}
	} else {
		taskAge := now.Sub(t.FinishedAt.Time())
		if t.IsSuccessful() {
			if taskAge >= CleanupSuccessfulTasksAfter {
				return true
			}
		} else {
			if taskAge >= CleanupFailedTasksAfter {
				return true
			}
		}
	}
	return false
}
