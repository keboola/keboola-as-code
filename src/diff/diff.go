package diff

type Change struct {
	Path string
	From interface{}
	To   interface{}
}
