package client

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"keboola-as-code/src/utils"
)

// Pool of the asynchronous HTTP requests. When processing a response, a new request can be send.
type Pool struct {
	id              int
	logger          *zap.SugaredLogger
	client          *Client            // resty client
	ctx             context.Context    // context of the parallel work
	workers         *errgroup.Group    // error group -> if one worker fails, all will be stopped
	activeRequests  sync.WaitGroup     // detect when all requests are processed (count of the requests = count of the processed responses)
	sendersCount    int                // number of parallel http connections -> value of MaxIdleConns
	processorsCount int                // number of processors workers -> number of CPUs
	requestsCount   *utils.SafeCounter // number of send requests
	requests        []*Request         // to check that the Send () method has been called on all requests
	requestsLock    *sync.Mutex        // lock for access to requests slice
	startTime       time.Time          // time when StartAndWait() called
	doneChan        chan struct{}      // channel for "all requests are processed" notification
	requestsChan    chan *Request      // channel for requests
	responsesChan   chan *Response     // channel for outgoing responses
}

func (c *Client) NewPool(logger *zap.SugaredLogger) *Pool {
	workers, ctx := errgroup.WithContext(c.parentCtx)
	return &Pool{
		id:              c.poolIdCounter.IncAndGet(),
		client:          c,
		logger:          logger,
		ctx:             ctx,
		workers:         workers,
		activeRequests:  sync.WaitGroup{},
		sendersCount:    MaxIdleConns,
		processorsCount: runtime.NumCPU(),
		requestsCount:   utils.NewSafeCounter(0),
		requestsLock:    &sync.Mutex{},
		doneChan:        make(chan struct{}),
		requestsChan:    make(chan *Request, 100),
		responsesChan:   make(chan *Response, 1),
	}
}

func (p *Pool) SetContext(ctx context.Context) {
	p.ctx = ctx
}

// Request set request sender to pool.
func (p *Pool) Request(request *Request) *Request {
	request.sender = p
	p.requestsLock.Lock()
	p.requests = append(p.requests, request)
	p.requestsLock.Unlock()
	return request
}

// Send adds request to pool.
func (p *Pool) Send(request *Request) {
	request.SetContext(p.ctx)
	request.sender = p
	request.sent = true
	p.logRequestState("queued", request, nil)
	p.activeRequests.Add(1)
	p.requestsChan <- request
}

func (p *Pool) StartAndWait() error {
	p.startTime = time.Now()
	p.start()
	err := p.wait()
	if err == nil {
		p.checkAllRequestsSent()
	}
	return err
}

// Wait until all requests done.
func (p *Pool) wait() error {
	defer close(p.responsesChan)
	defer close(p.requestsChan)
	return p.workers.Wait()
}

func (p *Pool) start() {
	// Work is done -> all responses are processed
	go func() {
		defer close(p.doneChan)
		p.activeRequests.Wait()
		if p.requestsCount.Get() > 0 {
			p.log("all done | %s", time.Since(p.startTime))
		}
	}()

	// Start senders
	for i := 0; i < p.sendersCount; i++ {
		p.workers.Go(func() error {
			for {
				var request *Request

				// Receive request from channel
				select {
				case <-p.doneChan:
					// All done -> end
					return nil
				case <-p.ctx.Done():
					// Context closed -> some error -> end
					return nil
				case request = <-p.requestsChan:
				}

				// Send request and write response to channel
				select {
				case <-p.doneChan:
					// All done -> end
					return nil
				case <-p.ctx.Done():
					// Context closed -> some error -> end
					return nil
				case p.responsesChan <- p.send(request):
					continue
				}
			}
		})
	}

	// Start processors
	for i := 0; i < p.processorsCount; i++ {
		p.workers.Go(func() error {
			for {
				select {
				case <-p.doneChan:
					// All done -> end
					return nil
				case <-p.ctx.Done():
					// Context closed -> some error -> end
					return nil
				case response := <-p.responsesChan:
					if err := p.process(response); err != nil {
						// Error when processing response
						return err
					}
				}
			}
		})
	}
}

func (p *Pool) send(request *Request) *Response {
	p.logRequestState("started", request, nil)
	p.requestsCount.Inc()
	p.client.Send(request)
	p.logRequestState("finished", request, request.Err())
	return request.Response
}

func (p *Pool) process(response *Response) (err error) {
	defer p.activeRequests.Done() // mark request processed
	defer p.logRequestState("processed", response.Request, err)
	return response.Err()
}

func (p *Pool) logRequestState(state string, request *Request, err error) {
	msg := fmt.Sprintf("[%d] %s %s %s", request.Id(), state, request.Method, urlForLog(request.Request))
	if err != nil {
		msg += fmt.Sprintf(", error: \"%s\"", err)
	}
	p.log("%s", msg)
}

func (p *Pool) log(template string, args ...interface{}) {
	args = append([]interface{}{p.id}, args...)
	p.logger.Debugf("HTTP-POOL\t[%d]"+template, args...)
}

func (p *Pool) checkAllRequestsSent() {
	for _, request := range p.requests {
		if !request.sent {
			panic(fmt.Errorf("request[%d] %s \"%s\" was not sent - Send() method was not called", request.Id(), request.Method, request.URL))
		}
	}
}
