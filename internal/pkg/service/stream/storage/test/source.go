package test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

const (
	SourceType = definition.SourceType("test")
)

func NewSource(k key.SourceKey) definition.Source {
	return definition.Source{
		SourceKey:   k,
		Type:        SourceType,
		Name:        "My Source",
		Description: "My Description",
	}
}

func NewHTTPSource(k key.SourceKey) definition.Source {
	return definition.Source{
		SourceKey:   k,
		Type:        definition.SourceTypeHTTP,
		Name:        "My Source",
		Description: "My Description",
		HTTP:        &definition.HTTPSource{Secret: "012345678901234567890123456789012345678912345678"},
	}
}
