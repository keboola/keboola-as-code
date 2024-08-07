package plugin

import "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"

// RegisterSinkWithLocalStorage registers a decision-making function.
// If the function returns true, the sink has enabled support for local storage.
// This means, that actions on File/Slice entities are linked to the Sink entity events.
func (p *Plugins) RegisterSinkWithLocalStorage(fn func(sinkType definition.SinkType) bool) {
	p.localStorageSinks = append(p.localStorageSinks, fn)
}

// IsSinkWithLocalStorage returns true, if the sink supports local storage,
// see RegisterSinkWithLocalStorage for details.
func (p *Plugins) IsSinkWithLocalStorage(sinkType definition.SinkType) bool {
	for _, fn := range p.localStorageSinks {
		if fn(sinkType) {
			return true
		}
	}
	return false
}
