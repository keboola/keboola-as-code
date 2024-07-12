package recordctx

import (
	"context"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/valyala/fasthttp"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type fastHTTPContext struct {
	timestamp     time.Time
	req           *fasthttp.RequestCtx
	lock          sync.Mutex
	clientIP      net.IP
	headersMap    *orderedmap.OrderedMap
	headersString *string
	bodyString    *string
	bodyStringErr error
	bodyBytes     []byte
	bodyBytesErr  error
	bodyMap       *orderedmap.OrderedMap
	bodyMapErr    error
}

func FromFastHTTP(timestamp time.Time, req *fasthttp.RequestCtx) Context {
	return &fastHTTPContext{
		timestamp: timestamp,
		req:       req,
	}
}

func (c *fastHTTPContext) Ctx() context.Context {
	return c.req
}

func (c *fastHTTPContext) Timestamp() time.Time {
	return c.timestamp
}

func (c *fastHTTPContext) ClientIP() net.IP {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.clientIP == nil {
		c.clientIP = c.req.RemoteIP()
	}
	return c.clientIP
}

func (c *fastHTTPContext) HeadersString() string {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.headersString == nil {
		var lines []string
		for _, k := range c.req.Request.Header.PeekKeys() {
			k := string(k)
			lines = append(lines, http.CanonicalHeaderKey(k)+": "+string(c.req.Request.Header.Peek(k))+"\n")
		}
		sort.Strings(lines)
		return strings.Join(lines, "")
	}
	return *c.headersString
}

func (c *fastHTTPContext) HeadersMap() *orderedmap.OrderedMap {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.headersMap == nil {
		c.headersMap = c.headersToMap()
	}
	return c.headersMap
}

func (c *fastHTTPContext) BodyString() (string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.bodyString == nil && c.bodyStringErr == nil {
		if bytes, err := c.bodyBytesWithoutLock(); err == nil {
			v := string(bytes)
			c.bodyString = &v
		} else {
			c.bodyStringErr = err
		}
	}

	if c.bodyStringErr != nil {
		return "", c.bodyStringErr
	}

	return *c.bodyString, nil
}

func (c *fastHTTPContext) BodyBytes() ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.bodyBytesWithoutLock()
}

func (c *fastHTTPContext) BodyMap() (*orderedmap.OrderedMap, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.bodyMap == nil && c.bodyMapErr == nil {
		if bodyBytes, err := c.bodyBytesWithoutLock(); err != nil {
			c.bodyMapErr = err
		} else if bodyMap, err := parseBody(string(c.req.Request.Header.ContentType()), bodyBytes); err != nil {
			c.bodyMapErr = errors.PrefixError(err, "cannot parse request body")
		} else {
			c.bodyMap = bodyMap
		}
	}

	return c.bodyMap, c.bodyMapErr
}

func (c *fastHTTPContext) bodyBytesWithoutLock() ([]byte, error) {
	if c.bodyBytes == nil && c.bodyBytesErr == nil {
		c.bodyBytes, c.bodyBytesErr = io.ReadAll(c.req.RequestBodyStream())
	}
	return c.bodyBytes, c.bodyBytesErr
}

func (c *fastHTTPContext) headersToMap() *orderedmap.OrderedMap {
	out := orderedmap.New()
	for _, k := range c.req.Request.Header.PeekKeys() {
		k := string(k)
		out.Set(http.CanonicalHeaderKey(k), string(c.req.Request.Header.Peek(k)))
	}
	out.SortKeys(func(keys []string) {
		sort.Strings(keys)
	})
	return out
}
