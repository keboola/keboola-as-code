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
	// Signal returns the OTLP signal type ("logs", "metrics", "traces") for
	// OTLP records, or "" for plain HTTP records.
	Signal() string
	HeadersString() string
	HeadersMap() *orderedmap.OrderedMap
	ReleaseBuffers()
	BodyBytes() ([]byte, error)
	BodyLength() int
	BodyMap() (*orderedmap.OrderedMap, error)
	JSONValue(parserPool *fastjson.ParserPool) (*fastjson.Value, error)
}
