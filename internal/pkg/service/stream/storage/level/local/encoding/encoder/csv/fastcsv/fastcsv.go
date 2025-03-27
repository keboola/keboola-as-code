package

// Package fastcsv provides parallel writer/formatter for CSV records.
//   - There is WritersPool with fixed number of writers.
//   - The io.Writer.Write method of the out writer is always called with the whole CSV row.
//   - There is no built-in synchronization of the output, so the io.Writer.Write method must be protected externally.
//   - The values are always quoted, so it is possible to generate row in single pass, without scanning the column value.
//   - The resulting CSV is therefore a bit larger, but we rely on the compression that may follow in the writers chain.
fastcsv

import (
	"io"
	"runtime"
	"sync"

	"github.com/c2h5oh/datasize"
)

type WritersPool struct {
	// sem limits the number of parallel writers (by default the limit is the number of CPUs)
	// struct{} is a token, number of the tokens in the channel buffer match number of free writers
	sem  chan struct{}
	pool *sync.Pool
}

func NewWritersPool(out io.Writer, rowSizeLimit datasize.ByteSize, writers int) *WritersPool {
	if writers <= 0 {
		writers = runtime.GOMAXPROCS(0)
	}

	p := &WritersPool{}
	p.pool = &sync.Pool{
		New: func() any {
			return newWriter(out, rowSizeLimit)
		},
	}

	p.sem = make(chan struct{}, writers)
	for range writers {
		p.sem <- struct{}{}
	}

	return p
}

func (p *WritersPool) WriteRow(cols *[]any) (int, error) {
	// The algorithm below is more efficient than
	// if we send a pointer to a free writer directly through the channel.
	//
	// The deadlock.Pool is internally implemented using per-processor local pools.
	// When goroutine is scheduled to run on a specific thread associated with specific processor
	// and will try to retrieve an object from the pool, deadlock.Pool will first look in the current processor local pool.
	// Read more: https://victoriametrics.com/blog/tsdb-performance-techniques-sync-pool/
	<-p.sem
	w := p.pool.Get().(*writer)
	n, err := w.WriteRow(cols)
	p.pool.Put(w)
	p.sem <- struct{}{}
	return n, err
}
