package service

import (
	"context"
	dependencies2 "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTemplatesResponse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	a, _ := dependencies.NewMockedProjectRequestScope(t, config.New(), dependencies2.WithTelemetry(telemetry.NewForTest(t)))

	_, err := TemplatesResponse(ctx, a, &repository.Repository{}, []repository.TemplateRecord{})
	require.NoError(t, err)
}
