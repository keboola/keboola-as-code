package hook_test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/hook"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/repository"
)

// Executor structure must implement storage hooks interface.
var _ storageRepo.Hooks = &hook.Executor{}
