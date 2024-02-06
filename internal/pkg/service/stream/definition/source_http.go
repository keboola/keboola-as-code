package definition

const (
	SourceTypeHTTP = SourceType("http")
)

type HTTPSource struct {
	Secret string `json:"secret" validate:"required,len=48"`
}

func (v SourceType) String() string {
	return string(v)
}
