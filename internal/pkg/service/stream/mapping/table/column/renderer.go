package column

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gofrs/uuid/v5"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/valyala/fastjson"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	jsonnetWrapper "github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Renderer struct {
	jsonnetPool  *jsonnetWrapper.VMPool[recordctx.Context]
	fastjsonPool *fastjson.ParserPool
}

func NewRenderer() *Renderer {
	return &Renderer{
		jsonnetPool:  jsonnet.NewPool(),
		fastjsonPool: &fastjson.ParserPool{},
	}
}

func (r *Renderer) CSVValue(c Column, ctx recordctx.Context) (string, error) {
	switch c := c.(type) {
	case Body:
		return ctx.BodyString()
	case Datetime:
		// Time is always in UTC, time format has fixed length
		return ctx.Timestamp().UTC().Format(TimeFormat), nil
	case Headers:
		return json.EncodeString(ctx.HeadersMap(), false)
	case UUID:
		id, err := uuid.NewV7()
		if err != nil {
			return "", err
		}

		return id.String(), err
	case IP:
		return ctx.ClientIP().String(), nil
	case Path:
		contentType := ctx.HeadersMap().GetOrNil("Content-Type")

		if contentType == "application/json" {
			return r.jsonPathCSVValue(c, ctx)
		}

		return r.mapPathCSVValue(c, ctx)
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

func (r *Renderer) mapPathCSVValue(c Path, ctx recordctx.Context) (string, error) {
	bodyMap, err := ctx.BodyMap()
	if err != nil {
		return "", err
	}

	var value any

	if c.Path == "" {
		value = bodyMap
	} else {
		value, _, err = bodyMap.GetNested(c.Path)
		if err != nil {
			if c.DefaultValue != nil {
				value = *c.DefaultValue
			} else {
				return "", errors.Wrapf(err, `path "%s" not found in the body`, c.Path)
			}
		}
	}

	if c.RawString {
		if stringValue, ok := value.(string); ok {
			return stringValue, nil
		}
	}

	return json.EncodeString(value, false)
}

func (r *Renderer) jsonPathCSVValue(c Path, ctx recordctx.Context) (string, error) {
	body, err := ctx.BodyBytes()
	if err != nil {
		return "", err
	}

	parser := r.fastjsonPool.Get()
	defer r.fastjsonPool.Put(parser)

	value, err := parser.ParseBytes(body)
	if err != nil {
		return "", err
	}

	path := orderedmap.PathFromStr(c.Path)

	keys := []string{}
	for _, step := range path {
		var key string

		switch step := step.(type) {
		case orderedmap.MapStep:
			key = step.Key()
		case orderedmap.SliceStep:
			key = strconv.Itoa(step.Index())
		}

		keys = append(keys, key)
	}

	var result any
	var resultErr error

	if len(keys) > 0 {
		if value.Exists(keys...) {
			value = value.Get(keys...)
		} else {
			resultErr = errors.New(fmt.Sprintf(`path "%s" not found in the body`, c.Path))
		}
	}

	if resultErr == nil {
		// Optimize TypeObject to avoid json decode and re-encode.
		if value.Type() == fastjson.TypeObject {
			return value.GetObject().String(), nil
		}

		result, resultErr = r.getJSONValue(value)
	} else if c.DefaultValue != nil {
		result = *c.DefaultValue
		resultErr = nil
	}

	if resultErr != nil {
		return "", resultErr
	}

	if c.RawString {
		if stringValue, ok := result.(string); ok {
			return stringValue, nil
		}
	}

	return json.EncodeString(result, false)
}

func (r *Renderer) getJSONValue(value *fastjson.Value) (any, error) {
	switch value.Type() {
	case fastjson.TypeObject:
		var result any
		err := json.DecodeString(value.GetObject().String(), &result)
		return result, err
	case fastjson.TypeArray:
		result := []any{}
		for _, v := range value.GetArray() {
			jsonValue, err := r.getJSONValue(v)
			if err != nil {
				return nil, err
			}
			result = append(result, jsonValue)
		}
		return result, nil
	case fastjson.TypeString:
		return string(value.GetStringBytes()), nil
	case fastjson.TypeNumber:
		return value.GetFloat64(), nil
	case fastjson.TypeTrue:
		return true, nil
	case fastjson.TypeFalse:
		return false, nil
	case fastjson.TypeNull:
		return nil, nil
	}

	return nil, errors.New("Unexpected fastjson type")
}
