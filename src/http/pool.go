package http

import (
	"context"
	"github.com/go-resty/resty/v2"
	"golang.org/x/sync/errgroup"
	"runtime"
	"sync"
)

// Pool of the asynchronous HTTP requests. When processing a response, a new request can be send.
type Pool struct {
	client          *Client         // resty client
	ctx             context.Context // context of the parallel work
	workers         *errgroup.Group // error group -> if one worker fails, all will be stopped
	counter         sync.WaitGroup  // detect when all requests are processed (count of the requests = count of the processed responses)
	sendersCount    int
	processorsCount int
	processor       func(pool *Pool, response *PoolResponse) error // callback to process response
	done            chan struct{}                                  // channel for "all requests are processed" notification
	requests        chan *resty.Request
	responses       chan *PoolResponse
}

type PoolResponse struct {
	response *resty.Response
	err      error
}

func (r *PoolResponse) HasResponse() bool {
	return r.response != nil
}

func (r *PoolResponse) HasError() bool {
	return r.err != nil
}

func (r *PoolResponse) Response() *resty.Response {
	return r.response
}

func (r *PoolResponse) Error() error {
	return r.err
}

func (c *Client) NewPool(processor func(pool *Pool, response *PoolResponse) error) *Pool {
	workers, ctx := errgroup.WithContext(c.parentCtx)
	return &Pool{
		client:          c,
		ctx:             ctx,
		workers:         workers,
		counter:         sync.WaitGroup{},
		sendersCount:    MaxIdleConns,
		processorsCount: runtime.NumCPU(),
		processor:       processor,
		done:            make(chan struct{}),
		requests:        make(chan *resty.Request, 100),
		responses:       make(chan *PoolResponse, 1),
	}
}

// Req creates request
func (p *Pool) Req(method string, url string) *resty.Request {
	return p.client.Req(method, url).SetContext(p.ctx)
}

// Send adds request to pool
func (p *Pool) Send(request *resty.Request) {
	p.counter.Add(1)
	p.requests <- request
}

// Wait until all requests done
func (p *Pool) Wait() error {
	defer close(p.responses)
	defer close(p.requests)
	return p.workers.Wait()
}

func (p *Pool) Start() {
	// Work is done -> all responses are processed
	go func() {
		defer close(p.done)
		p.counter.Wait()
	}()

	// Start senders
	for i := 0; i < p.sendersCount; i++ {
		p.workers.Go(func() error {
			for {
				select {
				case <-p.done:
					// All done -> end
					return nil
				case <-p.ctx.Done():
					// Context closed -> some error -> end
					return nil
				case request := <-p.requests:
					p.responses <- p.send(request)
				}
			}
		})
	}

	// Start processors
	for i := 0; i < p.processorsCount; i++ {
		p.workers.Go(func() error {
			for {
				select {
				case <-p.done:
					// All done -> end
					return nil
				case <-p.ctx.Done():
					// Context closed -> some error -> end
					return nil
				case response := <-p.responses:
					if err := p.process(response); err != nil {
						// Error when processing response
						return err
					}
				}
			}
		})
	}
}

func (p *Pool) send(request *resty.Request) *PoolResponse {
	response, err := request.SetContext(p.ctx).Send()
	return &PoolResponse{response, err}
}

func (p *Pool) process(response *PoolResponse) error {
	defer p.counter.Done() // mark request processed
	return p.processor(p, response)
}
