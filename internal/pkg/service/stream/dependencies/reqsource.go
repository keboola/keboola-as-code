package dependencies

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// sourceRequestScope implements SourceRequestScope interface.
type sourceRequestScope struct {
	BranchRequestScope
	sourceKey key.SourceKey
}

func NewSourceRequestScope(branchReqScp BranchRequestScope, source key.SourceID) (v SourceRequestScope) {
	return newSourceRequestScope(branchReqScp, source)
}

func newSourceRequestScope(branchReqScp BranchRequestScope, source key.SourceID) *sourceRequestScope {
	d := &sourceRequestScope{}
	d.BranchRequestScope = branchReqScp
	d.sourceKey = key.SourceKey{BranchKey: d.BranchKey(), SourceID: source}
	return d
}

func (v *sourceRequestScope) SourceKey() key.SourceKey {
	return v.sourceKey
}
