package mapper

type Mapper struct {
	bufferAPIHost string
}

type dependencies interface {
	BufferAPIHost() string
}

func NewMapper(d dependencies) *Mapper {
	return &Mapper{bufferAPIHost: d.BufferAPIHost()}
}
