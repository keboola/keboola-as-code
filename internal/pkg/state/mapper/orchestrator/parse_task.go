package orchestrator

import (
	"fmt"
	"strconv"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type taskParser struct {
	content *orderedmap.OrderedMap
}

func (p *taskParser) id() (int, error) {
	raw, found := p.content.Get(`id`)
	if !found {
		return 0, fmt.Errorf(`missing "id" key`)
	}
	value, ok := raw.(float64) // JSON int is float64, by default in Go
	if !ok {
		return 0, fmt.Errorf(`"id" must be int, found %T`, raw)
	}
	if _, err := strconv.Atoi(cast.ToString(value)); err != nil {
		return 0, fmt.Errorf(`"id" must be int, found "%+v"`, raw)
	}
	p.content.Delete(`id`)
	return int(value), nil
}

func (p *taskParser) name() (string, error) {
	raw, found := p.content.Get(`name`)
	if !found {
		return "", fmt.Errorf(`missing "name" key`)
	}
	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf(`"name" must be string, found %T`, raw)
	}
	if len(value) == 0 {
		return "", fmt.Errorf(`"name" cannot be empty`)
	}
	p.content.Delete(`name`)
	return value, nil
}

func (p *taskParser) phaseId() (int, error) {
	raw, found := p.content.Get(`phase`)
	if !found {
		return 0, fmt.Errorf(`missing "phase" key`)
	}
	value, ok := raw.(float64) // JSON int is float64, by default in Go
	if !ok {
		return 0, fmt.Errorf(`"phase" must be int, found %T`, raw)
	}
	if _, err := strconv.Atoi(cast.ToString(value)); err != nil {
		return 0, fmt.Errorf(`"phase" must be int, found "%+v"`, raw)
	}
	p.content.Delete(`phase`)
	return int(value), nil
}

func (p *taskParser) componentId() (model.ComponentId, error) {
	taskRaw, found := p.content.Get(`task`)
	if !found {
		return "", fmt.Errorf(`missing "task" key`)
	}
	task, ok := taskRaw.(*orderedmap.OrderedMap)
	if !ok {
		return "", fmt.Errorf(`"task" key must be object, found %T`, taskRaw)
	}
	raw, found := task.Get(`componentId`)
	if !found {
		return "", fmt.Errorf(`missing "task.componentId" key`)
	}
	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf(`"task.componentId" must be string, found %T`, raw)
	}
	if len(value) == 0 {
		return "", fmt.Errorf(`"task.componentId" cannot be empty`)
	}
	task.Delete(`componentId`)
	p.content.Set(`task`, task)
	return model.ComponentId(value), nil
}

func (p *taskParser) configId() (model.ConfigId, error) {
	taskRaw, found := p.content.Get(`task`)
	if !found {
		return "", fmt.Errorf(`missing "task" key`)
	}
	task, ok := taskRaw.(*orderedmap.OrderedMap)
	if !ok {
		return "", fmt.Errorf(`"task" key must be object, found %T`, taskRaw)
	}
	raw, found := task.Get(`configId`)
	if !found {
		return "", fmt.Errorf(`missing "task.configId" key`)
	}
	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf(`"task.configId" must be string, found %T`, raw)
	}
	if len(value) == 0 {
		return "", fmt.Errorf(`"task.configId" cannot be empty`)
	}
	task.Delete(`configId`)
	p.content.Set(`task`, task)
	return model.ConfigId(value), nil
}

func (p *taskParser) configPath() (string, error) {
	taskRaw, found := p.content.Get(`task`)
	if !found {
		return "", fmt.Errorf(`missing "task" key`)
	}
	task, ok := taskRaw.(*orderedmap.OrderedMap)
	if !ok {
		return "", fmt.Errorf(`"task" key must be object, found %T`, taskRaw)
	}
	raw, found := task.Get(`configPath`)
	if !found {
		return "", fmt.Errorf(`missing "task.configPath" key`)
	}
	value, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf(`"task.configPath" must be string, found %T`, raw)
	}
	if len(value) == 0 {
		return "", fmt.Errorf(`"task.configPath" cannot be empty`)
	}
	task.Delete(`configPath`)
	p.content.Set(`task`, task)
	return value, nil
}

func (p *taskParser) additionalContent() *orderedmap.OrderedMap {
	return p.content.Clone()
}
