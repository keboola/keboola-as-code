// Package fastcsv provides parallel writer/formatter for CSV records.
//   - There is WritersPool with fixed number of writers.
//   - The io.Writer.Write method of the out writer is always called with the whole CSV row.
//   - There is no built-in synchronization of the output, so the io.Writer.Write method must be protected externally.
//   - The values are always quoted, so it is possible to generate row in single pass, without scanning the column value.
//   - The resulting CSV is therefore a bit larger, but we rely on the compression that may follow in the writers chain.
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
