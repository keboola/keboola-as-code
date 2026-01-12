package twinformat

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
)

func TestComponentRegistry(t *testing.T) {
	t.Parallel()

	t.Run("empty registry", func(t *testing.T) {
		t.Parallel()
		registry := NewComponentRegistry()

		assert.Empty(t, registry.GetType("unknown-component"))
		assert.Empty(t, registry.GetName("unknown-component"))

		_, found := registry.Get("unknown-component")
		assert.False(t, found)
	})

	t.Run("register and get component", func(t *testing.T) {
		t.Parallel()
		registry := NewComponentRegistry()

		comp := &keboola.ComponentWithConfigs{
			Component: keboola.Component{
				ComponentKey: keboola.ComponentKey{ID: "keboola.ex-db-mysql"},
				Type:         "extractor",
				Name:         "MySQL",
			},
		}
		registry.Register(comp)

		assert.Equal(t, "extractor", registry.GetType("keboola.ex-db-mysql"))
		assert.Equal(t, "MySQL", registry.GetName("keboola.ex-db-mysql"))

		info, found := registry.Get("keboola.ex-db-mysql")
		assert.True(t, found)
		assert.Equal(t, "keboola.ex-db-mysql", info.ID)
		assert.Equal(t, "extractor", info.Type)
		assert.Equal(t, "MySQL", info.Name)
	})

	t.Run("register nil component", func(t *testing.T) {
		t.Parallel()
		registry := NewComponentRegistry()

		// Should not panic
		registry.Register(nil)

		assert.Empty(t, registry.GetType("anything"))
	})

	t.Run("multiple components", func(t *testing.T) {
		t.Parallel()
		registry := NewComponentRegistry()

		components := []*keboola.ComponentWithConfigs{
			{
				Component: keboola.Component{
					ComponentKey: keboola.ComponentKey{ID: "keboola.snowflake-transformation"},
					Type:         "transformation",
					Name:         "Snowflake SQL",
				},
			},
			{
				Component: keboola.Component{
					ComponentKey: keboola.ComponentKey{ID: "kds-team.app-custom-python"},
					Type:         "application",
					Name:         "Custom Python",
				},
			},
			{
				Component: keboola.Component{
					ComponentKey: keboola.ComponentKey{ID: "keboola.ex-shopify"},
					Type:         "extractor",
					Name:         "Shopify",
				},
			},
		}

		for _, comp := range components {
			registry.Register(comp)
		}

		assert.Equal(t, "transformation", registry.GetType("keboola.snowflake-transformation"))
		assert.Equal(t, "application", registry.GetType("kds-team.app-custom-python"))
		assert.Equal(t, "extractor", registry.GetType("keboola.ex-shopify"))

		assert.Equal(t, "Snowflake SQL", registry.GetName("keboola.snowflake-transformation"))
		assert.Equal(t, "Custom Python", registry.GetName("kds-team.app-custom-python"))
		assert.Equal(t, "Shopify", registry.GetName("keboola.ex-shopify"))
	})
}

func TestTableSourceRegistry(t *testing.T) {
	t.Parallel()

	t.Run("empty registry", func(t *testing.T) {
		t.Parallel()
		registry := NewTableSourceRegistry()

		assert.Equal(t, SourceUnknown, registry.GetSource("in.c-bucket.table"))

		_, found := registry.GetSourceInfo("in.c-bucket.table")
		assert.False(t, found)
	})

	t.Run("register and get source", func(t *testing.T) {
		t.Parallel()
		registry := NewTableSourceRegistry()

		registry.Register("out.c-bucket.table", TableSource{
			ComponentID:   "keboola.snowflake-transformation",
			ComponentType: "transformation",
			ConfigID:      "config-123",
			ConfigName:    "My Transformation",
		})

		assert.Equal(t, "keboola.snowflake-transformation", registry.GetSource("out.c-bucket.table"))

		info, found := registry.GetSourceInfo("out.c-bucket.table")
		assert.True(t, found)
		assert.Equal(t, "keboola.snowflake-transformation", info.ComponentID)
		assert.Equal(t, "transformation", info.ComponentType)
		assert.Equal(t, "config-123", info.ConfigID)
		assert.Equal(t, "My Transformation", info.ConfigName)
	})

	t.Run("register empty table ID", func(t *testing.T) {
		t.Parallel()
		registry := NewTableSourceRegistry()

		// Should not panic or register
		registry.Register("", TableSource{
			ComponentID: "some-component",
		})

		assert.Equal(t, SourceUnknown, registry.GetSource(""))
	})

	t.Run("multiple tables", func(t *testing.T) {
		t.Parallel()
		registry := NewTableSourceRegistry()

		registry.Register("out.c-customer-analysis.customers", TableSource{
			ComponentID:   "keboola.snowflake-transformation",
			ComponentType: "transformation",
			ConfigID:      "config-1",
			ConfigName:    "Customer Analysis",
		})
		registry.Register("out.c-customer-analysis.orders", TableSource{
			ComponentID:   "keboola.snowflake-transformation",
			ComponentType: "transformation",
			ConfigID:      "config-1",
			ConfigName:    "Customer Analysis",
		})
		registry.Register("out.c-hackernews.stories", TableSource{
			ComponentID:   "kds-team.app-custom-python",
			ComponentType: "application",
			ConfigID:      "config-2",
			ConfigName:    "Hacker News Extractor",
		})

		assert.Equal(t, "keboola.snowflake-transformation", registry.GetSource("out.c-customer-analysis.customers"))
		assert.Equal(t, "keboola.snowflake-transformation", registry.GetSource("out.c-customer-analysis.orders"))
		assert.Equal(t, "kds-team.app-custom-python", registry.GetSource("out.c-hackernews.stories"))
		assert.Equal(t, SourceUnknown, registry.GetSource("in.c-input.data"))
	})
}

func TestGetDominantSourceForBucket(t *testing.T) {
	t.Parallel()

	t.Run("no tables", func(t *testing.T) {
		t.Parallel()
		registry := NewTableSourceRegistry()

		result := registry.GetDominantSourceForBucket("out.c-bucket", []string{})
		assert.Equal(t, SourceUnknown, result)
	})

	t.Run("all tables unknown", func(t *testing.T) {
		t.Parallel()
		registry := NewTableSourceRegistry()

		result := registry.GetDominantSourceForBucket("out.c-bucket", []string{
			"out.c-bucket.table1",
			"out.c-bucket.table2",
		})
		assert.Equal(t, SourceUnknown, result)
	})

	t.Run("single source", func(t *testing.T) {
		t.Parallel()
		registry := NewTableSourceRegistry()

		registry.Register("out.c-bucket.table1", TableSource{ComponentID: "keboola.snowflake-transformation"})
		registry.Register("out.c-bucket.table2", TableSource{ComponentID: "keboola.snowflake-transformation"})

		result := registry.GetDominantSourceForBucket("out.c-bucket", []string{
			"out.c-bucket.table1",
			"out.c-bucket.table2",
		})
		assert.Equal(t, "keboola.snowflake-transformation", result)
	})

	t.Run("mixed sources - dominant wins", func(t *testing.T) {
		t.Parallel()
		registry := NewTableSourceRegistry()

		// 3 tables from transformation, 1 from python app
		registry.Register("out.c-bucket.table1", TableSource{ComponentID: "keboola.snowflake-transformation"})
		registry.Register("out.c-bucket.table2", TableSource{ComponentID: "keboola.snowflake-transformation"})
		registry.Register("out.c-bucket.table3", TableSource{ComponentID: "keboola.snowflake-transformation"})
		registry.Register("out.c-bucket.table4", TableSource{ComponentID: "kds-team.app-custom-python"})

		result := registry.GetDominantSourceForBucket("out.c-bucket", []string{
			"out.c-bucket.table1",
			"out.c-bucket.table2",
			"out.c-bucket.table3",
			"out.c-bucket.table4",
		})
		assert.Equal(t, "keboola.snowflake-transformation", result)
	})

	t.Run("mixed with unknown tables", func(t *testing.T) {
		t.Parallel()
		registry := NewTableSourceRegistry()

		// Only 1 known source, rest are unknown (not registered)
		registry.Register("out.c-bucket.table1", TableSource{ComponentID: "keboola.snowflake-transformation"})

		result := registry.GetDominantSourceForBucket("out.c-bucket", []string{
			"out.c-bucket.table1",
			"out.c-bucket.table2", // unknown
			"out.c-bucket.table3", // unknown
		})
		assert.Equal(t, "keboola.snowflake-transformation", result)
	})
}
