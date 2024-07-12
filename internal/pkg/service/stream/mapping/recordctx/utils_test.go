package recordctx

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
)

func TestParseBody_Json(t *testing.T) {
	t.Parallel()

	res, err := parseBody("application/json", []byte(`{"one":"two","three":"four"}`))
	assert.NoError(t, err)
	exp := orderedmap.New()
	exp.Set("one", "two")
	exp.Set("three", "four")
	assert.Equal(t, exp, res)
}

func TestParseBody_Form(t *testing.T) {
	t.Parallel()

	res, err := parseBody("application/x-www-form-urlencoded", []byte(`one=two&three=four`))
	assert.NoError(t, err)
	exp := orderedmap.New()
	exp.Set("one", "two")
	exp.Set("three", "four")
	assert.Equal(t, exp, res)
}

func TestParseBody_CustomJsonApi(t *testing.T) {
	t.Parallel()

	res, err := parseBody("application/foo.api+json", []byte(`{"one":"two","three":"four"}`))
	assert.NoError(t, err)
	exp := orderedmap.New()
	exp.Set("one", "two")
	exp.Set("three", "four")
	assert.Equal(t, exp, res)
}
