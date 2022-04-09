package diff

import (
	"math"
	"reflect"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Path []Step

type pathBuilder struct {
	steps        cmp.Path
	output       Path
	currentStep  cmp.PathStep
	currentIndex int
	lastIndex    int
	skipIndex    map[int]bool
}

func PathFromCmpPath(input cmp.Path) Path {
	p := pathBuilder{steps: input, lastIndex: len(input) - 1, skipIndex: make(map[int]bool)}
	p.build()
	return p.output
}

func (p Path) First() Step {
	if len(p) == 0 {
		return nil
	}
	return p[0]
}

func (p Path) Last() Step {
	if len(p) == 0 {
		return nil
	}
	return p[len(p)-1]
}

func (p Path) String() string {
	// Convert to string
	var parts []string
	for _, s := range p {
		parts = append(parts, s.String())
	}

	// Join
	out := strings.Join(parts, ".")

	// Simplify slice index after struct/index and vice versa
	out = strings.ReplaceAll(out, ".[", "[")
	out = strings.ReplaceAll(out, ".]", "]")
	return out
}

func (b *pathBuilder) build() {
	for b.currentIndex, b.currentStep = range b.steps {
		// Skip?
		if b.skipIndex[b.currentIndex] {
			continue
		}

		switch v := b.currentStep.(type) {
		case cmp.StructField:
			// Handle specials steps
			parentStep := b.steps[b.currentIndex-1]
			if outStep := b.stepKindOrNil(v); outStep != nil {
				b.add(outStep)
				continue
			} else if t := parentStep.Type(); t == reflect.TypeOf(model.ObjectNode{}) {
				// skip ObjectNode.Object/Children step
				continue
			}

			// Use name from the json tag if possible
			fieldName := v.Name()
			if field, found := parentStep.Type().FieldByName(v.Name()); found {
				if jsonName := field.Tag.Get("json"); jsonName != "" && jsonName != "-" {
					fieldName = jsonName
				}
			}
			b.add(newStepStructField(fieldName, v))
		case cmp.SliceIndex:
			// index1 or index2 can be "-1", if the value is on one side only
			index1, index2 := v.SplitKeys()
			index := cast.ToUint(math.Max(float64(index1), float64(index2)))
			b.add(newStepSliceIndex(index, v))
		case cmp.MapIndex:
			if outStep := b.stepObjectOrNil(v); outStep != nil {
				b.add(outStep)
			} else {
				index := cast.ToString(v.Key().Interface())
				b.add(newStepMapIndex(index, v))
			}
		}
	}
}

func (b *pathBuilder) add(step Step) {
	b.output = append(b.output, step)
}

func (b *pathBuilder) skip(stepIndex int) {
	b.skipIndex[stepIndex] = true
}

func (b *pathBuilder) stepObjectOrNil(step cmp.MapIndex) Step {
	// Previous and next steps must be defined
	prevIndex := b.currentIndex - 1
	if prevIndex < 0 {
		return nil
	}

	// Key must be model.Key
	key, ok := step.Key().Interface().(model.Key)
	if !ok {
		return nil
	}

	// Value must be *model.ObjectNode
	mapType := b.steps[prevIndex].Type()
	if mapType.Elem() != reflect.TypeOf(&model.ObjectNode{}) {
		return nil
	}

	return newStepObject(key, step)
}

// stepKindOrNil groups steps *model.ObjectNode[Children][model.Kind] together.
func (b *pathBuilder) stepKindOrNil(step cmp.StructField) Step {
	// Field name must be "Children"
	if step.Name() != "Children" {
		return nil
	}

	// Previous and next steps must be defined
	prevIndex := b.currentIndex - 1
	nextIndex := b.currentIndex + 1
	if prevIndex < 0 || nextIndex > b.lastIndex {
		return nil
	}

	// Parent type must be model.ObjectNode
	t := b.steps[prevIndex].Type()
	if t != reflect.TypeOf(model.ObjectNode{}) {
		return nil
	}

	// Next step must be MapIndex
	nextStep, ok := b.steps[nextIndex].(cmp.MapIndex)
	if !ok {
		return nil
	}

	// Map key must be kind
	kind, ok := nextStep.Key().Interface().(model.Kind)
	if !ok {
		return nil
	}

	b.skip(nextIndex)
	return newStepKind(kind, step)
}
