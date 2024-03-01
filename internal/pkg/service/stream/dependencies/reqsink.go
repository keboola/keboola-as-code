package dependencies

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// sinkRequestScope implements SinkRequestScope interface.
type sinkRequestScope struct {
	SourceRequestScope
	sinkKey key.SinkKey
}

func NewSinkRequestScope(sourceReqScp SourceRequestScope, sink key.SinkID) (v SinkRequestScope) {
	return newSinkRequestScope(sourceReqScp, sink)
}

func newSinkRequestScope(sourceReqScp SourceRequestScope, sink key.SinkID) *sinkRequestScope {
	d := &sinkRequestScope{}
	d.SourceRequestScope = sourceReqScp
	d.sinkKey = key.SinkKey{SourceKey: d.SourceKey(), SinkID: sink}
	return d
}

func (v *sinkRequestScope) SinkKey() key.SinkKey {
	return v.sinkKey
}
