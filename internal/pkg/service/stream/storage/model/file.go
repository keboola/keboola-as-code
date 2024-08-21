package model

import (
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	stagingModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/model"
	targetModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// File represents a file prepared in the staging storage to be imported into the target storage.
// File consists from zero or more Slices.
type File struct {
	FileKey
	Retryable
	Deleted     bool             `json:"-"` // internal field to mark the entity for deletion, there is no soft delete
	State       FileState        `json:"state" validate:"required,oneof=writing closing importing imported"`
	ClosingAt   *utctime.UTCTime `json:"closingAt,omitempty" validate:"excluded_if=State writing,required_if=State closing,required_if=State importing,required_if=State imported"`
	ImportingAt *utctime.UTCTime `json:"importingAt,omitempty" validate:"excluded_if=State writing,excluded_if=State closing,required_if=State importing,required_if=State imported"`
	ImportedAt  *utctime.UTCTime `json:"importedAt,omitempty"  validate:"excluded_if=State writing,excluded_if=State closing,excluded_if=State importing,required_if=State imported"`
	// Mapping defines how an incoming record is mapping to the result format, for example to a tabular data.
	Mapping table.Mapping `json:"mapping"` // in the future, here can be an interface - multiple mapping ways
	// Encoding defines how is the result format encoded, for example a tabular data to the CSV file.
	Encoding encoding.Config `json:"encoding"`
	// LocalStorage contains details of how the file is stored in the local storage.
	LocalStorage localModel.File `json:"local"`
	// StagingStorage contains details of how the file is uploaded to the staging storage.
	StagingStorage stagingModel.File `json:"staging"`
	// TargetStorage contains details of how the file is imported to the target storage.
	TargetStorage targetModel.Target `json:"target"`
}

type FileKey struct {
	key.SinkKey
	FileID
}

type FileID struct {
	OpenedAt utctime.UTCTime `json:"fileOpenedAt" validate:"required"`
}

func (v FileID) String() string {
	if v.OpenedAt.IsZero() {
		panic(errors.New("storage.FileID.OpenedAt cannot be empty"))
	}
	return v.OpenedAt.String()
}

func (v FileKey) String() string {
	return v.SinkKey.String() + "/" + v.FileID.String()
}

func (v FileKey) OpenedAt() utctime.UTCTime {
	return v.FileID.OpenedAt
}

func (f File) LastStateChange() utctime.UTCTime {
	switch {
	case f.ImportedAt != nil:
		return *f.ImportedAt
	case f.ImportingAt != nil:
		return *f.ImportingAt
	case f.ClosingAt != nil:
		return *f.ClosingAt
	default:
		return f.OpenedAt()
	}
}

func (f File) Telemetry() []attribute.KeyValue {
	lastStateChange := f.LastStateChange().Time()
	return []attribute.KeyValue{
		attribute.String("project.id", f.ProjectID.String()),
		attribute.String("branch.id", f.BranchID.String()),
		attribute.String("source.id", f.SourceID.String()),
		attribute.String("sink.id", f.SinkID.String()),
		attribute.String("file.id", f.FileID.String()),
		attribute.String("file.lastStateChange", lastStateChange.String()),
		attribute.Int("file.retryAttempt", f.RetryAttempt),
	}
}

func NewFileIDFromKey(key, prefix string) FileID {
	relativeKey := strings.TrimPrefix(key, prefix)
	openedAt, _, _ := strings.Cut(relativeKey, "/")
	return FileID{
		OpenedAt: utctime.MustParse(openedAt),
	}
}
