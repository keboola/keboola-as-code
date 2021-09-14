package plan

import "keboola-as-code/src/log"

type Plan interface {
	Name() string
	Log(writer *log.WriteCloser)
}
