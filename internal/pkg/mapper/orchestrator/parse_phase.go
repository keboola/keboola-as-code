package orchestrator

import (
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type phaseParser struct {
	content *orderedmap.OrderedMap
}

func (p *phaseParser) id() (string, error) {
	raw, found := p.content.Get(`id`)
	if !found {
		return "", errors.New(`missing "id" key`)
	}
	value, ok := raw.(string)
	if !ok {
		return "", errors.Errorf(`"id" must be string, found %T`, raw)
	}
	if len(value) == 0 {
		return "", errors.New(`"id" cannot be empty`)
	}
	p.content.Delete(`id`)
	return value, nil
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

func (p *phaseParser) dependsOnIds() ([]string, error) {
	var rawSlice []any
	raw, found := p.content.Get(`dependsOn`)
	if found {
		if v, ok := raw.([]any); ok {
			rawSlice = v
		}
	}

	// Convert []any -> []string
	value := make([]string, 0)
	for i, itemRaw := range rawSlice {
		if item, ok := itemRaw.(string); ok {
			if len(item) == 0 {
				return nil, errors.Errorf(`"dependsOn" cannot contain empty strings, found empty string at index %d`, i)
			}
			value = append(value, item)
		} else {
			return nil, errors.Errorf(`"dependsOn" key must contain only strings, found "%+v", index %d`, itemRaw, i)
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
