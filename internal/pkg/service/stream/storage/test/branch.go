package test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func NewBranch(k key.BranchKey) definition.Branch {
	return definition.Branch{BranchKey: k}
}
