package column

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	jsonnetWrapper "github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/receive/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/receive/receivectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Renderer struct {
	jsonnetPool *jsonnetWrapper.Pool
}

func NewRenderer() *Renderer {
	return &Renderer{
		jsonnetPool: jsonnet.NewPool(),
	}
}

func (r *Renderer) CSVValue(c Column, ctx *receivectx.Context) (string, error) {
	switch c := c.(type) {
	case Body:
		return ctx.Body, nil
	case Datetime:
		// Time is always in UTC, time format has fixed length
		return ctx.Now.UTC().Format(TimeFormat), nil
	case Headers:
		return json.EncodeString(ctx.HeadersMap(), false)
	case ID:
		return IDPlaceholder, nil
	case IP:
		return ctx.IP.String(), nil
	case Template:
		if c.Language != TemplateLanguageJsonnet {
			return "", errors.Errorf(`unsupported language "%s", only "jsonnet" is supported`, c.Language)
		}

		vm := r.jsonnetPool.Get()
		defer r.jsonnetPool.Put(vm)

		res, err := jsonnet.Evaluate(vm, ctx, c.Content)
		if err != nil {
			return res, err
		}

		return strings.TrimRight(res, "\n"), nil
	}

	return "", errors.Errorf("unknown column type %T", c)
}
