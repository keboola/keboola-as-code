package fastcsv

type ValueError struct {
	ColumnIndex int
	err         error
}

func (e ValueError) Error() string {
	return e.err.Error()
}

func (e ValueError) Unwrap() error {
	return e.err
}
