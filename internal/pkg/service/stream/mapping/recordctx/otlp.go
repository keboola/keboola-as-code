package recordctx

import (
	"context"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/valyala/fastjson"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type otlpContext struct {
	ctx       context.Context
	timestamp time.Time
	clientIP  net.IP
	headers   *orderedmap.OrderedMap
	bodyMap   *orderedmap.OrderedMap

	lock          sync.Mutex
	headersString *string
	bodyBytes     []byte
	bodyBytesErr  error
	jsonValue     *fastjson.Value
	jsonValueErr  error
}

// FromOTLP builds a Context from a single pre-flattened OTLP record body.
//
// timestamp is the request arrival time — the OTLP record's own timestamp
// stays inside bodyMap under "timestamp" so the column renderer can promote
// it to a dedicated column independently of the datetime column.
//
// headers is the original HTTP request headers map (pass through), since the
// OTLP transport rides on HTTP and downstream column mappings may extract
// values like User-Agent.
func FromOTLP(
	ctx context.Context,
	timestamp time.Time,
	clientIP net.IP,
	headers *orderedmap.OrderedMap,
	bodyMap *orderedmap.OrderedMap,
) Context {
	return &otlpContext{
		ctx:       ctx,
		timestamp: timestamp,
		clientIP:  clientIP,
		headers:   headers,
		bodyMap:   bodyMap,
	}
}

func (c *otlpContext) Ctx() context.Context {
	return c.ctx
}

func (c *otlpContext) Timestamp() time.Time {
	return c.timestamp
}

func (c *otlpContext) ClientIP() net.IP {
	return c.clientIP
}

func (c *otlpContext) HeadersString() string {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.headersString != nil {
		return *c.headersString
	}

	var s string
	if c.headers == nil {
		s = ""
	} else {
		keys := c.headers.Keys()
		lines := make([]string, 0, len(keys))
		for _, k := range keys {
			v, _ := c.headers.Get(k)
			if str, ok := v.(string); ok {
				lines = append(lines, http.CanonicalHeaderKey(k)+": "+str+"\n")
			}
		}
		sort.Strings(lines)
		s = strings.Join(lines, "")
	}
	c.headersString = &s
	return s
}

func (c *otlpContext) HeadersMap() *orderedmap.OrderedMap {
	if c.headers == nil {
		return orderedmap.New()
	}
	return c.headers
}

func (c *otlpContext) ReleaseBuffers() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.bodyBytes = nil
	c.jsonValue = nil
}

func (c *otlpContext) BodyBytes() ([]byte, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.bodyBytesWithoutLock()
}

func (c *otlpContext) BodyLength() int {
	b, err := c.BodyBytes()
	if err != nil {
		return 0
	}
	return len(b)
}

func (c *otlpContext) BodyMap() (*orderedmap.OrderedMap, error) {
	return c.bodyMap, nil
}

func (c *otlpContext) JSONValue(parserPool *fastjson.ParserPool) (*fastjson.Value, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.jsonValue != nil || c.jsonValueErr != nil {
		return c.jsonValue, c.jsonValueErr
	}

	body, err := c.bodyBytesWithoutLock()
	if err != nil {
		c.jsonValueErr = err
		return nil, err
	}

	parser := parserPool.Get()
	defer parserPool.Put(parser)

	if v, err := parser.ParseBytes(body); err != nil {
		c.jsonValueErr = errors.PrefixError(err, "cannot parse OTLP record JSON")
	} else {
		c.jsonValue = v
	}
	return c.jsonValue, c.jsonValueErr
}

func (c *otlpContext) bodyBytesWithoutLock() ([]byte, error) {
	if c.bodyBytes != nil || c.bodyBytesErr != nil {
		return c.bodyBytes, c.bodyBytesErr
	}
	if c.bodyMap == nil {
		c.bodyBytes = []byte("{}")
		return c.bodyBytes, nil
	}
	b, err := json.Marshal(c.bodyMap)
	if err != nil {
		c.bodyBytesErr = errors.PrefixError(err, "cannot serialize OTLP record body to JSON")
		return nil, c.bodyBytesErr
	}
	c.bodyBytes = b
	return c.bodyBytes, nil
}
