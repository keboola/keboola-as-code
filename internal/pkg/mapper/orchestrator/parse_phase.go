package orchestrator

import (
	"fmt"

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
	// Accept string or int/float64, always convert to string
	switch v := raw.(type) {
	case string:
		if len(v) == 0 {
			return "", errors.New(`"id" cannot be empty`)
		}
		p.content.Delete(`id`)
		return v, nil
	case int:
		p.content.Delete(`id`)
		return fmt.Sprintf("%d", v), nil
	case float64:
		p.content.Delete(`id`)
		return fmt.Sprintf("%.0f", v), nil
	default:
		return "", errors.Errorf(`"id" must be string or int, found %T`, raw)
	}
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

	// Convert []any -> []string, accept string or int/float64
	value := make([]string, 0)
	for i, itemRaw := range rawSlice {
		switch v := itemRaw.(type) {
		case string:
			if len(v) == 0 {
				return nil, errors.Errorf(`"dependsOn" cannot contain empty strings, found empty string at index %d`, i)
			}
			value = append(value, v)
		case int:
			value = append(value, fmt.Sprintf("%d", v))
		case float64:
			value = append(value, fmt.Sprintf("%.0f", v))
		default:
			return nil, errors.Errorf(`"dependsOn" key must contain only strings or ints, found %T, index %d`, itemRaw, i)
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
