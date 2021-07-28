package encryption

import (
	"fmt"
	"strings"
)

type path []step

type step interface {
	String() string
}

type mapStep string

type sliceStep int

func (v path) String() string {
	parts := make([]string, 0)
	for _, step := range v {
		parts = append(parts, step.String())
	}
	return strings.Join(parts, ".")
}

func (v mapStep) String() string {
	return string(v)
}

func (v sliceStep) String() string {
	return fmt.Sprintf("[%v]", int(v))
}
