package mapper

import (
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
)

func TestMapperReceiverPayloadToModel(t *testing.T) {
	t.Parallel()

	payload := buffer.CreateReceiverPayload{
		StorageAPIToken: "",
		ID:              nil,
		Name:            "Receiver",
		Exports: []*buffer.CreateExportData{
			{
				ID:   nil,
				Name: "Export",
				Mapping: &buffer.Mapping{
					TableID:     "in.c-bucket.table",
					Incremental: new(bool),
					Columns: []*buffer.Column{
						{
							Type:     "body",
							Template: nil,
						},
						{
							Type: "template",
							Template: &buffer.Template{
								Language:               "jsonnet",
								UndefinedValueStrategy: "null",
								Content:                `a+":"+b`,
								DataType:               "STRING",
							},
						},
					},
				},
				Conditions: &buffer.Conditions{
					Count: 1000,
					Size:  "100MB",
					Time:  "3m",
				},
			},
		},
	}

	model, err := ReceiverModelFromPayload(1000, payload)
	assert.NoError(t, err)
	wildcards.Assert(t,
		`{
  "receiverId": "receiver",
  "projectId": 1000,
  "name": "Receiver",
  "secret": "%s",
  "Exports": [
    {
      "exportId": "export",
      "name": "Export",
      "importConditions": {
        "count": 1000,
        "size": "100MB",
        "time": 180000000000
      },
      "revisionId": 0,
      "tableId": {
        "stage": "in",
        "bucketName": "bucket",
        "tableName": "table"
      },
      "incremental": false,
      "columns": [
        {
          "type": "body"
        },
        {
          "type": "template",
          "language": "jsonnet",
          "undefinedValueStrategy": "null",
          "content": "a+\":\"+b",
          "dataType": "STRING"
        }
      ]
    }
  ]
}
`,
		json.MustEncodeString(model, true),
	)
}

func TestMapperReceiverModelToPayload(t *testing.T) {
	t.Parallel()

	model := model.Receiver{
		ReceiverBase: model.ReceiverBase{
			ID:        "receiver",
			ProjectID: 1000,
			Name:      "Receiver",
			Secret:    "test",
		},
		Exports: []model.Export{
			{
				ExportBase: model.ExportBase{
					ID:   "export",
					Name: "Export",
					ImportConditions: model.ImportConditions{
						Count: 1000,
						Size:  100,
						Time:  100_000_000_000,
					},
				},
				Mapping: model.Mapping{
					RevisionID: 1,
					TableID: model.TableID{
						Stage:  model.TableStageIn,
						Bucket: "bucket",
						Table:  "table",
					},
					Incremental: false,
					Columns: column.Columns{
						column.Body{},
						column.Template{
							Language:               "jsonnet",
							UndefinedValueStrategy: "null",
							Content:                `a+":"+b`,
							DataType:               "STRING",
						},
					},
				},
			},
		},
	}

	payload := ReceiverPayloadFromModel("buffer.keboola.local", model)
	assert.Equal(t,
		`{
  "ID": "receiver",
  "URL": "https://buffer.keboola.local/v1/import/1000/receiver/test",
  "Name": "Receiver",
  "Exports": [
    {
      "ID": "export",
      "ReceiverID": "receiver",
      "Name": "Export",
      "Mapping": {
        "TableID": "in.c-bucket.table",
        "Incremental": false,
        "Columns": [
          {
            "Type": "body",
            "Template": null
          },
          {
            "Type": "template",
            "Template": {
              "Language": "jsonnet",
              "UndefinedValueStrategy": "null",
              "Content": "a+\":\"+b",
              "DataType": "STRING"
            }
          }
        ]
      },
      "Conditions": {
        "Count": 1000,
        "Size": "100B",
        "Time": "1m40s"
      }
    }
  ]
}
`,
		json.MustEncodeString(payload, true),
	)
}

func TestFormatUrl(t *testing.T) {
	t.Parallel()

	assert.Equal(
		t,
		"https://buffer.keboola.local/v1/import/1000/asdf/fdsa",
		formatReceiverURL("buffer.keboola.local", 1000, "asdf", "fdsa"),
	)
}
