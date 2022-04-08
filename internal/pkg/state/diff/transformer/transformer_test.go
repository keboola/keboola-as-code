package transformer_test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/state/sort"
)

func newDiffer() (A, B model.Objects, namingReg *naming.Registry, differ *diff.Differ) {
	namingReg = naming.NewRegistry()
	sorter := sort.NewPathSorter(namingReg)
	A = state.NewCollection(sorter)
	B = state.NewCollection(sorter)
	return A, B, namingReg, diff.NewDiffer(namingReg)
}
