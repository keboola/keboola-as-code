package fastcsv

import "github.com/c2h5oh/datasize"

type ValueError struct {
	ColumnIndex int
	err         error
}

type LimitError struct {
	ColumnIndex int
	Limit       datasize.ByteSize
	err         error
}

func (e ValueError) Error() string {
	return e.err.Error()
}

func (e ValueError) Unwrap() error {
	return e.err
}

func (e LimitError) Error() string {
	return e.err.Error()
}

func (e LimitError) Unwrap() error {
	return e.err
}
