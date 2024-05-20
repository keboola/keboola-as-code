package definition

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

type Branch struct {
	key.BranchKey
	Created
	Switchable
	SoftDeletable
	IsDefault bool `json:"isDefault"`
}
