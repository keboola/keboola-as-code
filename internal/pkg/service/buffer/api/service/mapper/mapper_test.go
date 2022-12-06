package mapper

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
)

func TestReceiverModelFromPayload(t *testing.T) {
	t.Parallel()

	mapper := NewMapper("buffer.keboola.local")

	projectID := 1000
	secret := idgenerator.ReceiverSecret()

	payload := buffer.CreateReceiverPayload{
		ID:   nil,
		Name: "Receiver",
		Exports: []*buffer.CreateExportData{
			{
				ID:   nil,
				Name: "Export",
				Mapping: &buffer.Mapping{
					TableID:     "in.c-bucket.table",
					Incremental: new(bool),
					Columns: []*buffer.Column{
						{
							Type: "body",
							Name: "body",
						},
						{
							Type: "template",
							Name: "template",
							Template: &buffer.Template{
								Language:               "jsonnet",
								UndefinedValueStrategy: "null",
								Content:                `a+":"+b`,
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

	receiverKey := key.ReceiverKey{
		ProjectID:  projectID,
		ReceiverID: "receiver",
	}
	exportKey := key.ExportKey{
		ReceiverKey: receiverKey,
		ExportID:    "export",
	}
	mappingKey := key.MappingKey{
		ExportKey:  exportKey,
		RevisionID: 1,
	}
	expected := model.Receiver{
		ReceiverBase: model.ReceiverBase{
			ReceiverKey: receiverKey,
			Name:        "Receiver",
			Secret:      secret,
		},
		Exports: []model.Export{
			{
				ExportBase: model.ExportBase{
					ExportKey: exportKey,
					Name:      "Export",
					ImportConditions: model.ImportConditions{
						Count: 1000,
						Size:  104857600,
						Time:  180_000_000_000,
					},
				},
				Mapping: model.Mapping{
					MappingKey: mappingKey,
					TableID: model.TableID{
						Stage:  model.TableStageIn,
						Bucket: "bucket",
						Table:  "table",
					},
					Incremental: false,
					Columns: column.Columns{
						column.Body{Name: "body"},
						column.Template{
							Name:                   "template",
							Language:               "jsonnet",
							UndefinedValueStrategy: "null",
							Content:                `a+":"+b`,
						},
					},
				},
			},
		},
	}

	model, err := mapper.ReceiverModelFromPayload(projectID, secret, payload)
	assert.NoError(t, err)
	assert.Equal(t, expected, model)
}

func TestReceiverPayloadFromModel(t *testing.T) {
	t.Parallel()

	mapper := NewMapper("buffer.keboola.local")

	receiverKey := key.ReceiverKey{
		ProjectID:  1000,
		ReceiverID: "receiver",
	}
	exportKey := key.ExportKey{
		ReceiverKey: receiverKey,
		ExportID:    "export",
	}
	mappingKey := key.MappingKey{
		ExportKey:  exportKey,
		RevisionID: 1,
	}
	model := model.Receiver{
		ReceiverBase: model.ReceiverBase{
			ReceiverKey: receiverKey,
			Name:        "Receiver",
			Secret:      "test",
		},
		Exports: []model.Export{
			{
				ExportBase: model.ExportBase{
					ExportKey: exportKey,
					Name:      "Export",
					ImportConditions: model.ImportConditions{
						Count: 1000,
						Size:  100,
						Time:  100_000_000_000,
					},
				},
				Mapping: model.Mapping{
					MappingKey: mappingKey,
					TableID: model.TableID{
						Stage:  model.TableStageIn,
						Bucket: "bucket",
						Table:  "table",
					},
					Incremental: false,
					Columns: column.Columns{
						column.Body{Name: "body"},
						column.Template{
							Name:                   "template",
							Language:               "jsonnet",
							UndefinedValueStrategy: "null",
							Content:                `a+":"+b`,
						},
					},
				},
			},
		},
	}

	expected := buffer.Receiver{
		ID:   "receiver",
		URL:  "https://buffer.keboola.local/v1/import/1000/receiver/test",
		Name: "Receiver",
		Exports: []*buffer.Export{
			{
				ID:         "export",
				ReceiverID: "receiver",
				Name:       "Export",
				Mapping: &buffer.Mapping{
					TableID:     "in.c-bucket.table",
					Incremental: new(bool),
					Columns: []*buffer.Column{
						{
							Type: "body",
							Name: "body",
						},
						{
							Type: "template",
							Name: "template",
							Template: &buffer.Template{
								Language:               "jsonnet",
								UndefinedValueStrategy: "null",
								Content:                "a+\":\"+b",
							},
						},
					},
				},
				Conditions: &buffer.Conditions{
					Count: 1000,
					Size:  "100B",
					Time:  "1m40s",
				},
			},
		},
	}

	payload := mapper.ReceiverPayloadFromModel(model)
	assert.Equal(t, expected, payload)
}

func TestFormatUrl(t *testing.T) {
	t.Parallel()

	assert.Equal(
		t,
		"https://buffer.keboola.local/v1/import/1000/asdf/fdsa",
		formatReceiverURL("buffer.keboola.local", 1000, "asdf", "fdsa"),
	)
}
