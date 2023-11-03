package definition

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
)

type Branch struct {
	key.BranchKey
	SoftDeletable
	IsDefault bool `json:"isDefault"`
}
