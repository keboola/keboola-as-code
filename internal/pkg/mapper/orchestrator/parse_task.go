package orchestrator

import (
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type taskParser struct {
	content *orderedmap.OrderedMap
}

func (p *taskParser) id() (string, error) {
	raw, found := p.content.Get(`id`)
	if !found {
		return "", errors.New(`missing "id" key`)
	}
	value, ok := raw.(string)
	if !ok {
		return "", errors.Errorf(`"id" must be string, found %v`, raw)
	}
	if len(value) == 0 {
		return "", errors.New(`"id" cannot be empty`)
	}
	p.content.Delete(`id`)
	return value, nil
}

func (p *taskParser) name() (string, error) {
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

func (p *taskParser) enabled() (bool, error) {
	raw, found := p.content.Get(`enabled`)
	if !found {
		// Use default value
		return true, nil
	}
	value, ok := raw.(bool)
	if !ok {
		return true, errors.Errorf(`"enabled" must be boolean, found %T`, raw)
	}
	p.content.Delete(`enabled`)
	return value, nil
}

func (p *taskParser) phaseID() (string, error) {
	raw, found := p.content.Get(`phase`)
	if !found {
		return "", errors.New(`missing "phase" key`)
	}
	value, ok := raw.(string)
	if !ok {
		return "", errors.Errorf(`"phase" must be string, found %v`, raw)
	}
	if len(value) == 0 {
		return "", errors.New(`"phase" cannot be empty`)
	}
	p.content.Delete(`phase`)
	return value, nil
}

func (p *taskParser) componentID() (keboola.ComponentID, error) {
	taskRaw, found := p.content.Get(`task`)
	if !found {
		return "", errors.New(`missing "task" key`)
	}
	task, ok := taskRaw.(*orderedmap.OrderedMap)
	if !ok {
		return "", errors.Errorf(`"task" key must be object, found %T`, taskRaw)
	}
	raw, found := task.Get(`componentId`)
	if !found {
		return "", errors.New(`missing "task.componentId" key`)
	}
	value, ok := raw.(string)
	if !ok {
		return "", errors.Errorf(`"task.componentId" must be string, found %T`, raw)
	}
	if len(value) == 0 {
		return "", errors.New(`"task.componentId" cannot be empty`)
	}
	task.Delete(`componentId`)
	p.content.Set(`task`, task)
	return keboola.ComponentID(value), nil
}

func (p *taskParser) hasConfigID() bool {
	taskRaw, found := p.content.Get(`task`)
	if !found {
		return false
	}
	task, ok := taskRaw.(*orderedmap.OrderedMap)
	if !ok {
		return false
	}
	value, found := task.Get(`configId`)
	return found && value != ""
}

func (p *taskParser) configID() (keboola.ConfigID, error) {
	taskRaw, found := p.content.Get(`task`)
	if !found {
		return "", errors.New(`missing "task" key`)
	}
	task, ok := taskRaw.(*orderedmap.OrderedMap)
	if !ok {
		return "", errors.Errorf(`"task" key must be object, found %T`, taskRaw)
	}
	raw, found := task.Get(`configId`)
	if !found {
		return "", errors.New(`missing "task.configId" key`)
	}
	value, ok := raw.(string)
	if !ok {
		return "", errors.Errorf(`"task.configId" must be string, found %T`, raw)
	}
	if len(value) == 0 {
		return "", errors.New(`"task.configId" cannot be empty`)
	}
	task.Delete(`configId`)
	p.content.Set(`task`, task)
	return keboola.ConfigID(value), nil
}

func (p *taskParser) hasConfigPath() bool {
	taskRaw, found := p.content.Get(`task`)
	if !found {
		return false
	}
	task, ok := taskRaw.(*orderedmap.OrderedMap)
	if !ok {
		return false
	}
	value, found := task.Get(`configPath`)
	return found && value != ""
}

func (p *taskParser) configPath() (string, error) {
	taskRaw, found := p.content.Get(`task`)
	if !found {
		return "", errors.New(`missing "task" key`)
	}
	task, ok := taskRaw.(*orderedmap.OrderedMap)
	if !ok {
		return "", errors.Errorf(`"task" key must be object, found %T`, taskRaw)
	}
	raw, found := task.Get(`configPath`)
	if !found {
		return "", errors.New(`missing "task.configPath" key`)
	}
	value, ok := raw.(string)
	if !ok {
		return "", errors.Errorf(`"task.configPath" must be string, found %T`, raw)
	}
	if len(value) == 0 {
		return "", errors.New(`"task.configPath" cannot be empty`)
	}
	task.Delete(`configPath`)
	p.content.Set(`task`, task)
	return value, nil
}

func (p *taskParser) hasConfigData() bool {
	taskRaw, found := p.content.Get(`task`)
	if !found {
		return false
	}
	task, ok := taskRaw.(*orderedmap.OrderedMap)
	if !ok {
		return false
	}
	_, found = task.Get(`configData`)
	return found
}

func (p *taskParser) configData() (*orderedmap.OrderedMap, error) {
	taskRaw, found := p.content.Get(`task`)
	if !found {
		return nil, errors.New(`missing "task" key`)
	}
	task, ok := taskRaw.(*orderedmap.OrderedMap)
	if !ok {
		return nil, errors.Errorf(`"task" key must be object, found %T`, taskRaw)
	}
	raw, found := task.Get(`configData`)
	if !found {
		return nil, errors.New(`missing "task.configData" key`)
	}
	value, ok := raw.(*orderedmap.OrderedMap)
	if !ok {
		return nil, errors.Errorf(`"task.configData" must be object, found %T`, raw)
	}
	task.Delete(`configData`)
	p.content.Set(`task`, task)
	return value, nil
}

func (p *taskParser) additionalContent() *orderedmap.OrderedMap {
	return p.content.Clone()
}
