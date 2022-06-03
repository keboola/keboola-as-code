package client

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const REQUESTS_BUFFER_SIZE = 10000

// Pool of the asynchronous HTTP requests. When processing a response, a new request can be send.
type Pool struct {
	id                  int
	started             bool
	finished            bool
	logger              log.Logger
	client              *Client            // resty client
	ctx                 context.Context    // context of the parallel work
	workers             *errgroup.Group    // error group -> if one worker fails, all will be stopped
	activeRequests      sync.WaitGroup     // detect when all requests are processed (count of the requests = count of the processed responses)
	sendersCount        int                // number of parallel http connections -> value of MaxIdleConns
	processorsCount     int                // number of processors workers -> number of CPUs
	requestsQueuedCount *utils.SafeCounter // number of total queued requests
	requestsSentCount   *utils.SafeCounter // number of sent requests
	requests            []*Request         // to check that the Send () method has been called on all requests
	requestsLock        *sync.Mutex        // lock for access to requests slice
	startTime           time.Time          // time when StartAndWait() called
	doneChan            chan struct{}      // channel for "all requests are processed" notification
	requestsChan        chan *Request      // channel for requests
	responsesChan       chan *Response     // channel for outgoing responses
}

func (c *Client) NewPool(logger log.Logger) *Pool {
	workers, ctx := errgroup.WithContext(c.parentCtx)
	return &Pool{
		id:                  c.poolIdCounter.IncAndGet(),
		client:              c,
		logger:              logger,
		ctx:                 ctx,
		workers:             workers,
		activeRequests:      sync.WaitGroup{},
		sendersCount:        MaxIdleConns,
		processorsCount:     runtime.NumCPU(),
		requestsQueuedCount: utils.NewSafeCounter(0),
		requestsSentCount:   utils.NewSafeCounter(0),
		requestsLock:        &sync.Mutex{},
		doneChan:            make(chan struct{}),
		requestsChan:        make(chan *Request, REQUESTS_BUFFER_SIZE),
		responsesChan:       make(chan *Response, 1),
	}
}

func (p *Pool) SetContext(ctx context.Context) {
	p.ctx = ctx
}

// Request set request sender to pool.
func (p *Pool) Request(request *Request) *Request {
	if request.sent {
		panic(fmt.Errorf("the request has already been sent"))
	}

	request.sender = p
	p.requestsLock.Lock()
	p.requests = append(p.requests, request)
	p.requestsLock.Unlock()
	return request
}

// Send adds request to pool.
func (p *Pool) Send(request *Request) {
	// Check if a free space is in the "requestsChan" channel.
	// If not, then a deadlock would occur. See test "TestPoolManyRequests".
	if !p.started && p.requestsQueuedCount.IncAndGet() >= REQUESTS_BUFFER_SIZE {
		panic(fmt.Errorf(`too many (%d) queued reuests in HTTP pool`, p.requestsQueuedCount.Get()))
	}

	// Check if is pool active
	if p.finished {
		request.Response = NewResponse(request, nil, fmt.Errorf("pool is finished"))
		return
	}

	request.SetContext(p.ctx)
	request.sender = p
	request.sent = true
	p.logRequestState("queued", request, nil)
	p.activeRequests.Add(1)
	p.requestsChan <- request
}

func (p *Pool) StartAndWait() error {
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
	defer func() { p.finished = true }()
	if err := p.workers.Wait(); err != nil {
		p.log("stopped on error: %s | %s", err, time.Since(p.startTime))
		return err
	}
	if p.requestsSentCount.Get() > 0 {
		p.log("all done | %s", time.Since(p.startTime))
	}
	return nil
}

func (p *Pool) start() {
	// Mark started
	if p.started {
		panic(fmt.Errorf(`Pool has been already started.`))
	}
	p.started = true
	p.startTime = time.Now()

	// Work is done -> all responses are processed -> close goroutines
	go func() {
		p.activeRequests.Wait()
		close(p.doneChan)
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
	p.requestsSentCount.Inc()
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
