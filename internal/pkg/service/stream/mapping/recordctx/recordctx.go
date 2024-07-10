// Package recordctx provides information about processing record.
package recordctx

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Context struct {
	Ctx     context.Context
	Now     time.Time
	IP      net.IP
	Headers http.Header
	Body    string

	// lazy
	headersMap   *orderedmap.OrderedMap
	bodyMap      *orderedmap.OrderedMap
	parseBodyErr error
}

func New(ctx context.Context, now time.Time, ip net.IP, headers http.Header, body string) *Context {
	return &Context{
		Ctx:     ctx,
		Now:     now,
		IP:      ip,
		Headers: headers,
		Body:    body,
	}
}

func (v *Context) HeadersStr() string {
	var lines []string
	for k, v := range v.Headers {
		lines = append(lines, fmt.Sprintf("%s: %s\n", http.CanonicalHeaderKey(k), v[0]))
	}
	sort.Strings(lines)
	return strings.Join(lines, "")
}

func (v *Context) HeadersMap() *orderedmap.OrderedMap {
	if v.headersMap == nil {
		v.headersMap = headersToMap(v.Headers)
	}
	return v.headersMap
}

func (v *Context) BodyMap() (*orderedmap.OrderedMap, error) {
	if v.parseBodyErr != nil || v.bodyMap != nil {
		return v.bodyMap, v.parseBodyErr
	}

	bodyMap, err := parseBody(v.Headers, v.Body)
	if err != nil {
		err = errors.Errorf("cannot parse request body: %w", err)
	}

	v.bodyMap, v.parseBodyErr = bodyMap, err
	return v.bodyMap, v.parseBodyErr
}

func (v *Context) ParseBodyErr() error {
	return v.parseBodyErr
}
