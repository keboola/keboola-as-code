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
	BodyString() (string, error)
	BodyBytes() ([]byte, error)
	BodyMap() (*orderedmap.OrderedMap, error)
	JSONValue(*fastjson.ParserPool) (*fastjson.Value, error)
}
