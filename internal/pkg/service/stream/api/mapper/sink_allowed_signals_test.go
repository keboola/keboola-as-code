package mapper_test

import (
	"net/url"
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
)

type testDeps struct{}

func (testDeps) APIPublicURL() *url.URL        { return mustParseURL("http://api.example.com") }
func (testDeps) HTTPSourcePublicURL() *url.URL { return mustParseURL("http://source.example.com") }

func mustParseURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

func newTestMapper() *mapper.Mapper {
	return mapper.New(testDeps{}, config.Config{})
}

func newMinimalTablePayload() *api.TableSinkCreate {
	tableType := definition.TableTypeKeboola
	return &api.TableSinkCreate{
		Type:    tableType,
		TableID: "in.c-bucket.my-table",
		Mapping: &api.TableMapping{
			Columns: api.TableColumns{
				{Type: column.Datetime{}.ColumnType(), Name: "datetime"},
			},
		},
	}
}

func newSourceKey() key.SourceKey {
	return key.SourceKey{
		BranchKey: key.BranchKey{ProjectID: 123, BranchID: 456},
		SourceID:  "my-source",
	}
}

func newTableSink() *definition.TableSink {
	return &definition.TableSink{
		Type: definition.TableTypeKeboola,
		Keboola: &definition.KeboolaTable{
			TableID: keboola.MustParseTableID("in.c-bucket.my-table"),
		},
	}
}

func TestNewSinkEntity_AllowedSignals(t *testing.T) {
	t.Parallel()
	m := newTestMapper()
	sourceKey := newSourceKey()

	t.Run("set_logs_only", func(t *testing.T) {
		t.Parallel()
		payload := &api.CreateSinkPayload{
			Name:           "logs-sink",
			Type:           definition.SinkTypeTable,
			AllowedSignals: []string{"logs"},
			Table:          newMinimalTablePayload(),
		}
		entity, err := m.NewSinkEntity(sourceKey, payload)
		require.NoError(t, err)
		assert.Equal(t, []string{"logs"}, entity.AllowedSignals)
	})

	t.Run("set_multiple_signals", func(t *testing.T) {
		t.Parallel()
		payload := &api.CreateSinkPayload{
			Name:           "multi-sink",
			Type:           definition.SinkTypeTable,
			AllowedSignals: []string{"logs", "metrics"},
			Table:          newMinimalTablePayload(),
		}
		entity, err := m.NewSinkEntity(sourceKey, payload)
		require.NoError(t, err)
		assert.Equal(t, []string{"logs", "metrics"}, entity.AllowedSignals)
	})

	t.Run("empty_means_all_signals", func(t *testing.T) {
		t.Parallel()
		payload := &api.CreateSinkPayload{
			Name:  "all-sink",
			Type:  definition.SinkTypeTable,
			Table: newMinimalTablePayload(),
		}
		entity, err := m.NewSinkEntity(sourceKey, payload)
		require.NoError(t, err)
		assert.Empty(t, entity.AllowedSignals)
	})
}

func TestUpdateSinkEntity_AllowedSignals(t *testing.T) {
	t.Parallel()
	m := newTestMapper()
	sinkType := definition.SinkTypeTable

	base := definition.Sink{
		SinkKey: key.SinkKey{SourceKey: newSourceKey(), SinkID: "my-sink"},
		Type:    definition.SinkTypeTable,
		Name:    "My Sink",
		Table:   newTableSink(),
	}

	t.Run("set_signals", func(t *testing.T) {
		t.Parallel()
		entity := base
		payload := &api.UpdateSinkPayload{
			Type:           &sinkType,
			AllowedSignals: []string{"traces"},
		}
		updated, err := m.UpdateSinkEntity(entity, payload)
		require.NoError(t, err)
		assert.Equal(t, []string{"traces"}, updated.AllowedSignals)
	})

	t.Run("nil_means_no_change", func(t *testing.T) {
		t.Parallel()
		entity := base
		entity.AllowedSignals = []string{"logs"}
		payload := &api.UpdateSinkPayload{
			Type:           &sinkType,
			AllowedSignals: nil,
		}
		updated, err := m.UpdateSinkEntity(entity, payload)
		require.NoError(t, err)
		assert.Equal(t, []string{"logs"}, updated.AllowedSignals)
	})

	t.Run("empty_slice_clears_filter", func(t *testing.T) {
		t.Parallel()
		entity := base
		entity.AllowedSignals = []string{"logs"}
		payload := &api.UpdateSinkPayload{
			Type:           &sinkType,
			AllowedSignals: []string{},
		}
		updated, err := m.UpdateSinkEntity(entity, payload)
		require.NoError(t, err)
		assert.Empty(t, updated.AllowedSignals)
	})
}

func TestNewSinkResponse_AllowedSignals(t *testing.T) {
	t.Parallel()
	m := newTestMapper()

	t.Run("signals_present_in_response", func(t *testing.T) {
		t.Parallel()
		entity := definition.Sink{
			SinkKey:        key.SinkKey{SourceKey: newSourceKey(), SinkID: "my-sink"},
			Type:           definition.SinkTypeTable,
			Name:           "My Sink",
			AllowedSignals: []string{"logs", "metrics"},
			Table:          newTableSink(),
		}
		resp, err := m.NewSinkResponse(entity)
		require.NoError(t, err)
		assert.Equal(t, []string{"logs", "metrics"}, resp.AllowedSignals)
	})

	t.Run("empty_signals_in_response", func(t *testing.T) {
		t.Parallel()
		entity := definition.Sink{
			SinkKey: key.SinkKey{SourceKey: newSourceKey(), SinkID: "my-sink"},
			Type:    definition.SinkTypeTable,
			Name:    "My Sink",
			Table:   newTableSink(),
		}
		resp, err := m.NewSinkResponse(entity)
		require.NoError(t, err)
		assert.Empty(t, resp.AllowedSignals)
	})
}
