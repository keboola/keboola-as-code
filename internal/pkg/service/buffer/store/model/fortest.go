package model

import (
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
)

func ReceiverForTest(receiverID string, exportsCount int, now time.Time) Receiver {
	if now.IsZero() {
		now, _ = time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	}

	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: key.ReceiverID(receiverID)}

	var exports []Export
	for i := 0; i < exportsCount; i++ {
		exportID := fmt.Sprintf("my-export-%03d", i)
		tableID := fmt.Sprintf("in.c-bucket.table%03d", i)
		export := ExportForTest(receiverKey, exportID, tableID, []column.Column{column.ID{Name: "id"}}, now)
		exports = append(exports, export)
	}

	return Receiver{
		ReceiverBase: ReceiverBase{
			ReceiverKey: receiverKey,
			Name:        "My Receiver",
			Secret:      "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
		},
		Exports: exports,
	}
}

func ExportForTest(receiverKey key.ReceiverKey, exportID, tableID string, columns []column.Column, now time.Time) Export {
	if now.IsZero() {
		now, _ = time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: key.ExportID(exportID)}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(now)}
	sliceKey := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(now)}

	mapping := MappingForTest(exportKey)
	mapping.TableID = keboola.MustParseTableID(tableID)
	mapping.Columns = columns
	return Export{
		ExportBase: ExportBase{
			ExportKey:        exportKey,
			Name:             "My Export 1",
			ImportConditions: DefaultImportConditions(),
		},
		Mapping: mapping,
		Token: Token{
			ExportKey:    exportKey,
			StorageToken: keboola.Token{Token: "my-token", ID: "1234"},
		},
		OpenedFile: File{
			FileKey:         fileKey,
			State:           filestate.Opened,
			Mapping:         mapping,
			StorageResource: &keboola.FileUploadCredentials{},
		},
		OpenedSlice: Slice{
			SliceKey:        sliceKey,
			State:           slicestate.Writing,
			Mapping:         mapping,
			StorageResource: &keboola.FileUploadCredentials{},
			Number:          1,
		},
	}
}

func MappingForTest(exportKey key.ExportKey) Mapping {
	return Mapping{
		MappingKey: key.MappingKey{ExportKey: exportKey, RevisionID: 1},
		TableID:    keboola.MustParseTableID("in.c-table.table1"),
		Columns:    []column.Column{column.ID{Name: "id"}},
	}
}
