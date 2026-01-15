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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/httputils"
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

func (r *Renderer) CSVValue(c Column, ctx recordctx.Context) (any, error) {
	switch c := c.(type) {
	case Body:
		return ctx.BodyBytes()
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
		contentType, ok := ctx.HeadersMap().GetOrNil("Content-Type").(string)

		if ok && httputils.IsContentTypeJSON(contentType) {
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

		res = strings.TrimRight(res, "\n")
		if c.RawString && res[0] == '"' {
			var decoded string
			if json.DecodeString(res, &decoded) == nil {
				return decoded, nil
			}
		}

		return res, nil
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
		return r.mapValueToString(value, c)
	}

	value, _, err = bodyMap.GetNested(c.Path)
	if err != nil {
		if c.DefaultValue != nil {
			value = *c.DefaultValue
		} else {
			return "", errors.Wrapf(err, `path "%s" not found in the body`, c.Path)
		}
	}

	return r.mapValueToString(value, c)
}

func (r *Renderer) mapValueToString(value any, c Path) (string, error) {
	if c.RawString {
		if stringValue, ok := value.(string); ok {
			return stringValue, nil
		}
	}

	return json.EncodeString(value, false)
}

func (r *Renderer) jsonPathCSVValue(c Path, ctx recordctx.Context) (string, error) {
	// Get fastjson.Value, needs to be cached in recordctx as it might be used in multiple columns
	value, err := ctx.JSONValue(r.fastjsonPool)
	if err != nil {
		return "", err
	}

	var resultErr error

	path := orderedmap.PathFromStr(c.Path)
	if len(path) > 0 {
		// Transform orderedmap.Path to a slice of keys used by fastjson
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

		// Fetch the desired value from the json
		if value.Exists(keys...) {
			value = value.Get(keys...)
		} else {
			resultErr = errors.New(fmt.Sprintf(`path "%s" not found in the body`, c.Path))
		}
	}

	if resultErr == nil {
		// Return unquoted string if the value is a string and RawString is set to true.
		if c.RawString && value.Type() == fastjson.TypeString {
			return string(value.GetStringBytes()), nil
		}

		// Return the found value (json encoded)
		return value.String(), nil
	} else if c.DefaultValue != nil {
		// An error happened while processing the path, but we have a DefaultValue to use.
		if c.RawString {
			return *c.DefaultValue, nil
		}

		return json.EncodeString(*c.DefaultValue, false)
	}

	return "", resultErr
}
