package format

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/state/diff"
)

const (
	EqualMark    = "="
	NotEqualMark = "*"
	AddMark      = "+"
	DeleteMark   = "×"
	OnlyInAMark  = "-"
	OnlyInBMark  = "+"
)

func mark(v diff.ResultState) string {
	switch v {
	case diff.ResultNotEqual:
		return NotEqualMark
	case diff.ResultEqual:
		return EqualMark
	case diff.ResultOnlyInA:
		return OnlyInAMark
	case diff.ResultOnlyInB:
		return OnlyInBMark
	default:
		panic(fmt.Errorf("unexpected value %#v", v))
	}
}
