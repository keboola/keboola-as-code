package api

import (
	"context"
	"github.com/go-resty/resty/v2"
	"golang.org/x/sync/errgroup"
	"runtime"
	"sync"
)

type Pool struct {
	client          *Client         // http client
	ctx             context.Context // context of the parallel work
	workers         *errgroup.Group // error group -> if one worker fails, all will be stopped
	counter         sync.WaitGroup  // detect when all requests are processed
	sendersCount    int
	processorsCount int
	processor       func(pool *Pool, response *resty.Response) error
	requests        chan *resty.Request
	responses       chan *resty.Response
}

func (c *Client) NewPool(processor func(pool *Pool, response *resty.Response) error) *Pool {
	workers, ctx := errgroup.WithContext(c.parentCtx)
	return &Pool{
		client:          c,
		ctx:             ctx,
		workers:         workers,
		counter:         sync.WaitGroup{},
		sendersCount:    MaxIdleConns,
		processorsCount: runtime.NumCPU(),
		processor:       processor,
		requests:        make(chan *resty.Request, 500),
		responses:       make(chan *resty.Response),
	}
}

// R creates request
func (p *Pool) R(method string, url string) *resty.Request {
	r := p.client.R().SetContext(p.ctx)
	r.Method = method
	r.URL = url
	return r
}

// Add request to pool
func (p *Pool) Add(request *resty.Request) {
	p.counter.Add(1)
	p.requests <- request
}

// Wait until all requests done
func (p *Pool) Wait() error {
	return p.workers.Wait()
}

func (p *Pool) Start() {
	// Close channels when all requests are processed
	p.workers.Go(func() error {
		defer close(p.requests)
		defer close(p.responses)
		p.counter.Wait()
		return nil
	})

	// Start senders
	for i := 0; i < p.sendersCount; i++ {
		p.workers.Go(func() error {
			// Send all requests, stop on error
			for request := range p.requests {
				if err := p.send(request); err != nil {
					return err
				}
			}

			return nil
		})
	}

	// Start processor
	for i := 0; i < p.processorsCount; i++ {
		p.workers.Go(func() error {
			// Process all responses, stop on error
			for response := range p.responses {
				if err := p.process(response); err != nil {
					return err
				}
			}

			return nil
		})
	}
}

func (p *Pool) send(request *resty.Request) error {
	response, err := request.SetContext(p.ctx).Send()
	if err != nil {
		p.counter.Done() // mark request processed
		return err
	}

	p.responses <- response
	return nil
}

func (p *Pool) process(response *resty.Response) error {
	defer p.counter.Done() // mark request processed
	return p.processor(p, response)
}
