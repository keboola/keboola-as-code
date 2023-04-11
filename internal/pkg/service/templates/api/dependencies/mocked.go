package dependencies

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/store"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type Mocked interface {
	dependencies.Mocked
	Schema() *schema.Schema
	Store() *store.Store
	TaskNode() *task.Node

	APIConfig() config.Config
	SetAPIConfigOps(ops ...config.Option)
}

type mocked struct {
	dependencies.Mocked
	t         *testing.T
	schema    *schema.Schema
	store     *store.Store
	taskNode  *task.Node
	apiConfig config.Config
}

func NewMockedDeps(t *testing.T, opts ...dependencies.MockedOption) Mocked {
	t.Helper()
	return &mocked{
		t:      t,
		Mocked: dependencies.NewMockedDeps(t, opts...),
		apiConfig: config.NewConfig().Apply(
			config.WithPublicAddress(&url.URL{Scheme: "https", Host: "templates.keboola.local"}),
		),
	}
}

func (v *mocked) Schema() *schema.Schema {
	if v.schema == nil {
		v.schema = schema.New(validator.New().Validate)
	}
	return v.schema
}

func (v *mocked) Store() *store.Store {
	if v.store == nil {
		v.store = store.New(v)
	}
	return v.store
}

func (v *mocked) TaskNode() *task.Node {
	if v.taskNode == nil {
		var err error
		v.taskNode, err = task.NewNode(v, task.WithSpanNamePrefix(config.SpanNamePrefix))
		assert.NoError(v.t, err)
	}
	return v.taskNode
}

func (v *mocked) SetAPIConfigOps(ops ...config.Option) {
	v.apiConfig = v.apiConfig.Apply(ops...)
}

func (v *mocked) APIConfig() config.Config {
	return v.apiConfig
}
