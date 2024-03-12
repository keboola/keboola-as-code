package hook_test

import (
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/hook"
)

// Executor structure must implement definition hooks interface.
var _ definitionRepo.Hooks = &hook.Executor{}
