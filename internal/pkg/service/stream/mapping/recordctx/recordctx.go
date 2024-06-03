// Package recordctx provides information about processing record.
package recordctx

import (
	"context"
	"net"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/valyala/fastjson"
)

type Context interface {
	Ctx() context.Context
	Timestamp() time.Time
	ClientIP() net.IP
	HeadersString() string
	HeadersMap() *orderedmap.OrderedMap
	ReleaseBuffers()
	BodyBytes() ([]byte, error)
	BodyLength() int
	BodyMap() (*orderedmap.OrderedMap, error)
	JSONValue(parserPool *fastjson.ParserPool) (*fastjson.Value, error)
}
