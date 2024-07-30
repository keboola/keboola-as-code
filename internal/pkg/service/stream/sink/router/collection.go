package router

import (
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

type collection struct {
	lock    sync.RWMutex
	sources map[key.SourceKey]*sourceData
}

type sourceData struct {
	sourceKey key.SourceKey
	sinks     map[key.SinkKey]*sinkData
}

type sinkData struct {
	sinkKey  key.SinkKey
	sinkType definition.SinkType
	enabled  bool
}

func newCollection() *collection {
	c := &collection{}
	c.reset()
	return c
}

func (c *collection) reset() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.sources = make(map[key.SourceKey]*sourceData)
}

func (c *collection) sourcesCount() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return len(c.sources)
}

func (c *collection) source(sourceKey key.SourceKey) (*sourceData, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	source, found := c.sources[sourceKey]
	return source, found
}

func (c *collection) sink(sinkKey key.SinkKey) (*sinkData, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	source := c.sources[sinkKey.SourceKey]
	if source == nil {
		return nil, false
	}

	sink := source.sinks[sinkKey]
	if sink == nil {
		return nil, false
	}

	return sink, true
}

func (c *collection) addSink(sink definition.Sink) {
	c.lock.Lock()
	defer c.lock.Unlock()

	source := c.sources[sink.SourceKey]
	if source == nil {
		source = &sourceData{sourceKey: sink.SourceKey, sinks: make(map[key.SinkKey]*sinkData)}
		c.sources[sink.SourceKey] = source
	}

	source.sinks[sink.SinkKey] = &sinkData{sinkKey: sink.SinkKey, sinkType: sink.Type, enabled: sink.IsEnabled()}
}

func (c *collection) deleteSink(sinkKey key.SinkKey) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if source := c.sources[sinkKey.SourceKey]; source != nil {
		delete(source.sinks, sinkKey)
		if len(source.sinks) == 0 {
			delete(c.sources, sinkKey.SourceKey)
		}
	}
}
