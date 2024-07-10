package column

import (
	"strings"

	"github.com/gofrs/uuid/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	jsonnetWrapper "github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Renderer struct {
	jsonnetPool *jsonnetWrapper.VMPool[recordctx.Context]
}

func NewRenderer() *Renderer {
	return &Renderer{
		jsonnetPool: jsonnet.NewPool(),
	}
}

func (r *Renderer) CSVValue(c Column, ctx *recordctx.Context) (string, error) {
	switch c := c.(type) {
	case Body:
		return ctx.Body, nil
	case Datetime:
		// Time is always in UTC, time format has fixed length
		return ctx.Now.UTC().Format(TimeFormat), nil
	case Headers:
		return json.EncodeString(ctx.HeadersMap(), false)
	case UUID:
		id, err := uuid.NewV7()
		if err != nil {
			return "", err
		}

		return id.String(), err
	case IP:
		return ctx.IP.String(), nil
	case Template:
		if c.Template.Language != TemplateLanguageJsonnet {
			return "", errors.Errorf(`unsupported language "%s", only "jsonnet" is supported`, c.Template.Language)
		}

		vm := r.jsonnetPool.Get()
		defer r.jsonnetPool.Put(vm)

		res, err := jsonnet.Evaluate(vm, ctx, c.Template.Content)
		if err != nil {
			return res, err
		}

		return strings.TrimRight(res, "\n"), nil
	}

	return "", errors.Errorf("unknown column type %T", c)
}
