package otlpsource

import (
	"context"
	"net/http"
	"sort"
	"strconv"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	routing "github.com/qiangxue/fasthttp-routing"
	"github.com/valyala/fasthttp"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source/dispatcher"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// ErrorHandler is the function signature used by httpsource to write errors.
// Re-typed here to avoid a cyclic import.
type ErrorHandler func(c *fasthttp.RequestCtx, err error)

// Handler implements the OTLP/HTTP endpoints. It is intentionally lightweight —
// state-free except for its dependencies — so a single instance can serve
// concurrent requests through the same fasthttp server as the HTTP source.
type Handler struct {
	ctx          context.Context
	logger       log.Logger
	clock        clockwork.Clock
	dispatcher   *dispatcher.Dispatcher
	errorHandler ErrorHandler
}

// New creates a Handler ready to serve OTLP requests.
//
// ctx is the long-lived service context (used to derive per-record contexts
// with tracing disabled, matching the HTTP source hot-path optimization).
func New(
	ctx context.Context,
	logger log.Logger,
	clock clockwork.Clock,
	dp *dispatcher.Dispatcher,
	errorHandler ErrorHandler,
) *Handler {
	return &Handler{
		ctx:          ctx,
		logger:       logger.WithComponent("otlp-source"),
		clock:        clock,
		dispatcher:   dp,
		errorHandler: errorHandler,
	}
}

// HandleLogs serves POST /v1/logs.
//
// On well-formed input it always returns HTTP 200 with an OTLP-conformant
// response body (possibly with partial_success). It only returns 4xx/5xx for
// transport-level problems: malformed body, unsupported encoding, missing
// source. This matches OTLP retry semantics: 4xx means "do not retry",
// 5xx means "retry the whole batch".
func (h *Handler) HandleLogs(c *routing.Context) error {
	projectID, sourceID, secret, err := parseAuthParams(c)
	if err != nil {
		h.errorHandler(c.RequestCtx, err)
		return nil //nolint:nilerr
	}

	enc := DetectEncoding(string(c.Request.Header.Peek("Content-Type")))
	if enc == EncodingUnsupported {
		h.errorHandler(c.RequestCtx, svcerrors.NewUnsupportedMediaTypeError(
			errors.New(`unsupported OTLP content type, expected "application/x-protobuf" or "application/json"`),
		))
		return nil
	}

	body, err := DecompressIfGzip(string(c.Request.Header.Peek("Content-Encoding")), c.Request.Body())
	if err != nil {
		h.errorHandler(c.RequestCtx, svcerrors.NewBadRequestError(err))
		return nil //nolint:nilerr
	}

	logs, err := DecodeLogs(body, enc)
	if err != nil {
		h.errorHandler(c.RequestCtx, svcerrors.NewBadRequestError(
			errors.PrefixError(err, "cannot decode OTLP logs payload"),
		))
		return nil //nolint:nilerr
	}

	records := FlattenLogs(logs)

	// Empty batches are valid per the OTLP spec — return 200 immediately.
	if len(records) == 0 {
		h.writeEmptySuccess(c, enc)
		return nil
	}

	ctx := telemetry.ContextWithDisabledTracing(h.ctx)
	headers := headersToOrderedMap(c.RequestCtx)
	result := DispatchRecords(
		ctx,
		h.dispatcher,
		h.clock.Now(),
		c.RequestCtx.RemoteIP(),
		headers,
		projectID,
		sourceID,
		secret,
		records,
	)

	encoded, err := BuildLogsResponse(enc, result)
	if err != nil {
		h.errorHandler(c.RequestCtx, err)
		return nil //nolint:nilerr
	}

	c.Response.SetStatusCode(encoded.StatusCode)
	if encoded.ContentType != "" {
		c.Response.Header.Set("Content-Type", encoded.ContentType)
	}
	c.Response.SetBody(encoded.Body)
	return nil
}

// HandleOptions serves OPTIONS for CORS preflight, matching the existing
// /stream/... pattern.
func (h *Handler) HandleOptions(c *routing.Context) error {
	c.Response.Header.Set("Allow", "OPTIONS, POST")
	c.Response.Header.Set("Access-Control-Allow-Methods", "OPTIONS, POST")
	c.Response.Header.Set("Access-Control-Allow-Headers", "*")
	c.Response.Header.Set("Access-Control-Expose-Headers", "*")
	c.Response.Header.Set("Access-Control-Allow-Origin", "*")
	c.Response.SetStatusCode(http.StatusOK)
	return nil
}

func (h *Handler) writeEmptySuccess(c *routing.Context, enc Encoding) {
	encoded, err := BuildLogsResponse(enc, DispatchResult{})
	if err != nil {
		h.errorHandler(c.RequestCtx, err)
		return
	}
	c.Response.SetStatusCode(encoded.StatusCode)
	c.Response.Header.Set("Content-Type", encoded.ContentType)
	c.Response.SetBody(encoded.Body)
}

func parseAuthParams(c *routing.Context) (keboola.ProjectID, key.SourceID, string, error) {
	projectIDStr := c.Param("projectID")
	projectIDInt, err := strconv.Atoi(projectIDStr)
	if err != nil {
		return 0, "", "", svcerrors.NewBadRequestError(errors.Errorf("invalid project ID %q", projectIDStr))
	}
	return keboola.ProjectID(projectIDInt), key.SourceID(c.Param("sourceID")), c.Param("secret"), nil
}

func headersToOrderedMap(reqCtx *fasthttp.RequestCtx) *orderedmap.OrderedMap {
	out := orderedmap.New()
	for _, k := range reqCtx.Request.Header.PeekKeys() {
		key := string(k)
		out.Set(http.CanonicalHeaderKey(key), string(reqCtx.Request.Header.Peek(key)))
	}
	out.SortKeys(func(keys []string) {
		sort.Strings(keys)
	})
	return out
}
