package service

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestTokenRefresh(t *testing.T) {
	t.Parallel()

	// Create service connected to a testing project
	prj := testproject.GetTestProjectForTest(t)
	d := dependencies.NewMockedDeps(t, dependencies.WithTestProject(prj))
	s := New(d)

	// Test ....
	s.CreateReceiver(d, &buffer.CreateReceiverPayload{})
}
