package plan

import "github.com/keboola/keboola-as-code/internal/pkg/log"

type Plan interface {
	Name() string
	Log(writer *log.WriteCloser)
}
