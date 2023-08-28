package writechain

type closeFn func() error

func (v closeFn) Close() error {
	return v()
}
