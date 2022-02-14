package deepcopy

import (
	"fmt"
	"strings"
)

type Steps []fmt.Stringer

func (s Steps) Add(step fmt.Stringer) Steps {
	newIndex := len(s)
	out := make(Steps, newIndex+1)
	copy(out, s)
	out[newIndex] = step
	return out
}

func (s Steps) String() string {
	var out []string
	for _, item := range s {
		out = append(out, item.String())
	}
	str := strings.Join(out, `.`)
	str = strings.ReplaceAll(str, `*.`, `*`)
	str = strings.ReplaceAll(str, `.[`, `[`)
	return str
}

type TypeStep struct {
	currentType string
}

func (v TypeStep) String() string {
	return v.currentType
}

type PointerStep struct{}

func (v PointerStep) String() string {
	return "*"
}

type InterfaceStep struct {
	targetType string
}

func (v InterfaceStep) String() string {
	return fmt.Sprintf("interface[%s]", v.targetType)
}

type StructFieldStep struct {
	currentType string
	field       string
}

func (v StructFieldStep) String() string {
	return fmt.Sprintf("%s[%s]", v.currentType, v.field)
}

type SliceIndexStep struct {
	index int
}

func (v SliceIndexStep) String() string {
	return fmt.Sprintf("slice[%d]", v.index)
}

type MapKeyStep struct {
	key interface{}
}

func (v MapKeyStep) String() string {
	return fmt.Sprintf("map[%v]", v.key)
}
