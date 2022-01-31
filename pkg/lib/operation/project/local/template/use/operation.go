package use

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Inputs []InputValue

type InputValue struct {
	Key   string
	Value interface{}
}

type Options struct {
	TargetBranch model.BranchKey
	Inputs       Inputs
}

type dependencies interface{}

func LoadStateOptions() loadState.Options {
	return loadState.Options{
		LoadLocalState:          true,
		LoadRemoteState:         false,
		IgnoreNotFoundErr:       false,
		IgnoreInvalidLocalState: false,
	}
}

func Run(o Options, d dependencies) error {
	return fmt.Errorf("not implemented")
}
