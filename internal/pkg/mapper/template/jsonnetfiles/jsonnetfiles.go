package jsonnetfiles

type jsonNetMapper struct {
	variables map[string]interface{}
}

func NewMapper(variables map[string]interface{}) *jsonNetMapper {
	return &jsonNetMapper{variables: variables}
}
