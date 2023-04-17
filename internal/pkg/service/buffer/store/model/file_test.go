package model

import (
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

func TestFile_Filename(t *testing.T) {
	t.Parallel()
	now, _ := time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	receiverKey := key.ReceiverKey{ProjectID: keboola.ProjectID(123), ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export"}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(now)}
	file := File{FileKey: fileKey}
	assert.Equal(t, "my-receiver_my-export_20060101080405", file.Filename())
}

func TestSlice_Filename(t *testing.T) {
	t.Parallel()
	now, _ := time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	receiverKey := key.ReceiverKey{ProjectID: keboola.ProjectID(123), ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export"}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(now)}
	sliceKey := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(now)}
	slice := Slice{SliceKey: sliceKey}
	assert.Equal(t, "20060101080405.gz", slice.Filename())
}
