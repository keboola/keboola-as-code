// Package condition evaluates slice upload and file import conditions.
//
// # Intervals
//   - [config.WorkerConfig.CheckConditionsInterval] defines how often the conditions are checked.
//   - [config.WorkerConfig.MinimalUploadInterval] defines minimal interval between two slice uploads per export.
//   - [config.WorkerConfig.MinimalImportInterval] defines minimal interval between two file imports per export.
//
// # Checker
//
// Conditions are evaluated by the [Checker], which runs on each worker node.
//   - Each instance of the [Checker] handles a part of the receivers, the work is divided using [distribution.Node].
//   - Each instance of the [Checker] stores in its memory data for evaluating the conditions for its part of the receivers.
//   - The cache is synchronized via the etcd Watch API.
//   - The cache is invalidated every time the distribution of worker nodes changes.
package condition

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

type Conditions = model.Conditions
