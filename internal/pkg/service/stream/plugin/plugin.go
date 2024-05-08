package plugin

type Plugins struct {
	collection *Collection
	executor   *Executor
}

type fnList[T any] []T

func New() *Plugins {
	c := &Collection{}
	e := &Executor{collection: c}
	return &Plugins{collection: c, executor: e}
}

func (fns fnList[T]) forEach(do func(T)) {
	for _, fn := range fns {
		do(fn)
	}
}

func (p *Plugins) Collection() *Collection {
	return p.collection
}

func (p *Plugins) Executor() *Executor {
	return p.executor
}
