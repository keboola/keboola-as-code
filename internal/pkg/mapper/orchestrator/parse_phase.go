package orchestrator

import (
	"strconv"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type phaseParser struct {
	content *orderedmap.OrderedMap
}

func (p *phaseParser) id() (int, error) {
	raw, found := p.content.Get(`id`)
	if !found {
		return 0, errors.New(`missing "id" key`)
	}
	value, ok := raw.(float64) // JSON int is float64, by default in Go
	if !ok {
		return 0, errors.Errorf(`"id" must be int, found %T`, raw)
	}
	if _, err := strconv.Atoi(cast.ToString(value)); err != nil {
		return 0, errors.Errorf(`"id" must be int, found "%+v"`, raw)
	}
	p.content.Delete(`id`)
	return int(value), nil
}

func (p *phaseParser) name() (string, error) {
	raw, found := p.content.Get(`name`)
	if !found {
		return "", errors.New(`missing "name" key`)
	}
	value, ok := raw.(string)
	if !ok {
		return "", errors.Errorf(`"name" must be string, found %T`, raw)
	}
	if len(value) == 0 {
		return "", errors.New(`"name" cannot be empty`)
	}
	p.content.Delete(`name`)
	return value, nil
}

func (p *phaseParser) dependsOnIds() ([]int, error) {
	var rawSlice []any
	raw, found := p.content.Get(`dependsOn`)
	if found {
		if v, ok := raw.([]any); ok {
			rawSlice = v
		}
	}

	// Convert []any -> []int
	value := make([]int, 0)
	for i, itemRaw := range rawSlice {
		if item, ok := itemRaw.(float64); ok { // JSON int is float64, by default in Go
			if _, err := strconv.Atoi(cast.ToString(item)); err != nil {
				return nil, errors.Errorf(`"dependsOn" must be int, found "%+v", index %d`, itemRaw, i)
			}
			value = append(value, int(item))
		} else {
			return nil, errors.Errorf(`"dependsOn" key must contain only integers, found "%+v", index %d`, itemRaw, i)
		}
	}

	p.content.Delete(`dependsOn`)
	return value, nil
}

func (p *phaseParser) dependsOnPaths() ([]string, error) {
	var rawSlice []any
	raw, found := p.content.Get(`dependsOn`)
	if found {
		if v, ok := raw.([]any); ok {
			rawSlice = v
		}
	}

	// Convert []any -> []string
	value := make([]string, 0)
	for i, item := range rawSlice {
		if itemStr, ok := item.(string); ok {
			value = append(value, itemStr)
		} else {
			return nil, errors.Errorf(`"dependsOn" key must contain only strings, found %T, index %d`, itemStr, i)
		}
	}

	p.content.Delete(`dependsOn`)
	return value, nil
}

func (p *phaseParser) additionalContent() *orderedmap.OrderedMap {
	return p.content.Clone()
}
