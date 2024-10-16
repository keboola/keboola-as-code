package result

import "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync/notify"

// WriteRecordResult returns result of WriteRecord method.
type WriteRecordResult struct {
	N        int
	Notifier *notify.Notifier
}

func NewNotifierWriteRecordResult(notifier *notify.Notifier) WriteRecordResult {
	return WriteRecordResult{
		Notifier: notifier,
	}
}
