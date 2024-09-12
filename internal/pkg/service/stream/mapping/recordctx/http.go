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
	"github.com/valyala/fastjson"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ip"
)

type httpContext struct {
	timestamp     time.Time
	req           *http.Request
	lock          sync.Mutex
	clientIP      net.IP
	headersMap    *orderedmap.OrderedMap
	headersString *string
	bodyBytes     []byte
	bodyBytesErr  error
	bodyMap       *orderedmap.OrderedMap
	bodyMapErr    error
	jsonValue     *fastjson.Value
	jsonValueErr  error
}

func FromHTTP(timestamp time.Time, req *http.Request) Context {
	return &httpContext{
		timestamp: timestamp,
		req:       req,
	}
}

func (c *httpContext) Ctx() context.Context {
	return c.req.Context()
}

func (c *httpContext) Timestamp() time.Time {
	return c.timestamp
}

func (c *httpContext) ClientIP() net.IP {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.clientIP == nil {
		c.clientIP = ip.From(c.req)
	}
	return c.clientIP
}

func (c *httpContext) HeadersString() string {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.headersString == nil {
		var lines []string
		for k, v := range c.req.Header {
			lines = append(lines, http.CanonicalHeaderKey(k)+": "+v[0]+"\n")
		}
		sort.Strings(lines)
		return strings.Join(lines, "")
	}
	return *c.headersString
}

func (c *httpContext) HeadersMap() *orderedmap.OrderedMap {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.headersMap == nil {
		c.headersMap = c.headersToMap()
	}
	return c.headersMap
}

func (c *httpContext) BodyBytes() ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.bodyBytesWithoutLock()
}

func (c *httpContext) BodyMap() (*orderedmap.OrderedMap, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.bodyMap == nil && c.bodyMapErr == nil {
		if bodyBytes, err := c.bodyBytesWithoutLock(); err != nil {
			c.bodyMapErr = err
		} else if bodyMap, err := parseBody(c.req.Header.Get("Content-Type"), bodyBytes); err != nil {
			c.bodyMapErr = errors.PrefixError(err, "cannot parse request body")
		} else {
			c.bodyMap = bodyMap
		}
	}

	return c.bodyMap, c.bodyMapErr
}

func (c *httpContext) JSONValue(parserPool *fastjson.ParserPool) (*fastjson.Value, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.jsonValue == nil && c.jsonValueErr == nil {
		if body, err := c.bodyBytesWithoutLock(); err != nil {
			c.jsonValueErr = err
		} else {
			parser := parserPool.Get()
			defer parserPool.Put(parser)

			if jsonValue, err := parser.ParseBytes(body); err != nil {
				c.jsonValueErr = errors.PrefixError(err, "cannot parse request json")
			} else {
				c.jsonValue = jsonValue
			}
		}
	}

	return c.jsonValue, c.jsonValueErr
}

func (c *httpContext) bodyBytesWithoutLock() ([]byte, error) {
	if c.bodyBytes == nil && c.bodyBytesErr == nil {
		c.bodyBytes, c.bodyBytesErr = io.ReadAll(c.req.Body)
	}
	return c.bodyBytes, c.bodyBytesErr
}

func (c *httpContext) headersToMap() *orderedmap.OrderedMap {
	out := orderedmap.New()
	for k, v := range c.req.Header {
		out.Set(http.CanonicalHeaderKey(k), v[0])
	}
	out.SortKeys(func(keys []string) {
		sort.Strings(keys)
	})
	return out
}
