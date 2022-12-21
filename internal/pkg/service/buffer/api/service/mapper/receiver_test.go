package mapper_test

import (
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/service/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestReceiverModel(t *testing.T) {
	t.Parallel()

	d := dependencies.NewMockedDeps(t)
	mapper := NewMapper(d)

	projectID := key.ProjectID(1000)
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
					TableID: storageapi.TableID{
						BucketID: storageapi.BucketID{
							Stage:      storageapi.BucketStageIn,
							BucketName: "bucket",
						},
						TableName: "table",
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

	out, err := mapper.CreateReceiverModel(projectID, secret, payload)
	assert.NoError(t, err)
	assert.Equal(t, expected, out)
}

func TestReceiverPayload(t *testing.T) {
	t.Parallel()

	d := dependencies.NewMockedDeps(t)
	mapper := NewMapper(d)

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
	out := model.Receiver{
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
					TableID: storageapi.TableID{
						BucketID: storageapi.BucketID{
							Stage:      storageapi.BucketStageIn,
							BucketName: "bucket",
						},
						TableName: "table",
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

	expected := &buffer.Receiver{
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

	payload := mapper.ReceiverPayload(out)
	assert.Equal(t, expected, payload)
}
