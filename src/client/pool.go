package client

import (
	"context"
	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"runtime"
	"sync"
)

// Pool of the asynchronous HTTP requests. When processing a response, a new request can be send.
type Pool struct {
	client          *Client // resty client
	logger          *zap.SugaredLogger
	ctx             context.Context // context of the parallel work
	workers         *errgroup.Group // error group -> if one worker fails, all will be stopped
	counter         sync.WaitGroup  // detect when all requests are processed (count of the requests = count of the processed responses)
	sendersCount    int
	processorsCount int
	processor       func(pool *Pool, response *PoolResponse) error // callback to process response
	done            chan struct{}                                  // channel for "all requests are processed" notification
	requests        chan *Request
	responses       chan *PoolResponse
}

type PoolResponse struct {
	request  *Request
	response *resty.Response
	err      error
}

func (r *PoolResponse) HasResponse() bool {
	return r.response != nil
}

func (r *PoolResponse) HasError() bool {
	return r.err != nil
}

func (r *PoolResponse) Request() *Request {
	return r.request
}

func (r *PoolResponse) Response() *resty.Response {
	return r.response
}

func (r *PoolResponse) Error() error {
	return r.err
}

func (r *PoolResponse) Url() string {
	return r.response.Request.URL
}

func NewPoolResponse(request *Request, response *resty.Response, err error) *PoolResponse {
	return &PoolResponse{request, response, err}
}

func (c *Client) NewPool(logger *zap.SugaredLogger, processor func(pool *Pool, response *PoolResponse) error) *Pool {
	workers, ctx := errgroup.WithContext(c.parentCtx)
	return &Pool{
		client:          c,
		logger:          logger,
		ctx:             ctx,
		workers:         workers,
		counter:         sync.WaitGroup{},
		sendersCount:    MaxIdleConns,
		processorsCount: runtime.NumCPU(),
		processor:       processor,
		done:            make(chan struct{}),
		requests:        make(chan *Request, 100),
		responses:       make(chan *PoolResponse, 1),
	}
}

// Req creates request
func (p *Pool) Req(method string, url string) *Request {
	return NewRequest(p.client.Req(method, url).SetContext(p.ctx))
}

// Send adds request to pool
func (p *Pool) Send(request *Request) {
	p.log("queued \"%s\"", request.Request().URL)
	p.counter.Add(1)
	p.requests <- request
}

func (p *Pool) StartAndWait() error {
	p.start()
	return p.wait()
}

// Wait until all requests done
func (p *Pool) wait() error {
	defer close(p.responses)
	defer close(p.requests)
	return p.workers.Wait()
}

func (p *Pool) start() {
	// Work is done -> all responses are processed
	go func() {
		defer close(p.done)
		p.counter.Wait()
		p.log("all done")
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
					// Wait for send and write to responses
					select {
					case <-p.done:
						// All done -> end
						return nil
					case <-p.ctx.Done():
						// Context closed -> some error -> end
						return nil
					case p.responses <- p.send(request):
						continue
					}
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

func (p *Pool) send(request *Request) (response *PoolResponse) {
	defer func() {
		if !response.HasError() {
			p.log("finished \"%s\"", request.Url())
		} else {
			p.log("failed \"%s\", error:\"%s\"", request.Url(), response.Error())
		}
	}()

	p.log("started \"%s\"", request.Url())
	restyResponse, err := p.client.Send(request.SetContext(p.ctx))
	response = NewPoolResponse(request, restyResponse, err)
	return response
}

func (p *Pool) process(response *PoolResponse) (err error) {
	defer p.counter.Done() // mark request processed
	defer func() {
		if err == nil {
			p.log("processed \"%s\"", response.Request().Url())
		} else {
			p.log("processed \"%s\", error:\"%s\"", response.Request().Url(), err)
		}
	}()

	err = p.processor(p, response)
	return err
}

func (p *Pool) log(template string, args ...interface{}) {
	p.logger.Debugf("HTTP-POOL\t"+template, args...)
}
