package fastcsv

import (
	"io"
	"runtime"
	"sync"
)

type WritersPool struct {
	sem  chan struct{}
	pool *sync.Pool
}

func NewWritersPool(out io.Writer, writers int) *WritersPool {
	if writers <= 0 {
		writers = runtime.GOMAXPROCS(0)
	}

	p := &WritersPool{}
	p.pool = &sync.Pool{
		New: func() any {
			return newWriter(out)
		},
	}

	p.sem = make(chan struct{}, writers)
	for i := 0; i < writers; i++ {
		p.sem <- struct{}{}
	}

	return p
}

func (p *WritersPool) WriteRow(cols *[]any) error {
	<-p.sem
	w := p.pool.Get().(*writer)
	err := w.WriteRow(cols)
	p.pool.Put(w)
	p.sem <- struct{}{}
	return err
}
