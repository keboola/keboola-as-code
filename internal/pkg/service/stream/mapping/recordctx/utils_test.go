package recordctx

import (
	"net/http"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
)

func TestParseBody_Json(t *testing.T) {
	t.Parallel()

	res, err := parseBody(http.Header{"Content-Type": []string{"application/json"}}, `{"one":"two","three":"four"}`)
	assert.NoError(t, err)
	exp := orderedmap.New()
	exp.Set("one", "two")
	exp.Set("three", "four")
	assert.Equal(t, exp, res)
}

func TestParseBody_Form(t *testing.T) {
	t.Parallel()

	res, err := parseBody(http.Header{"Content-Type": []string{"application/x-www-form-urlencoded"}}, `one=two&three=four`)
	assert.NoError(t, err)
	exp := orderedmap.New()
	exp.Set("one", "two")
	exp.Set("three", "four")
	assert.Equal(t, exp, res)
}

func TestParseBody_CustomJsonApi(t *testing.T) {
	t.Parallel()

	res, err := parseBody(http.Header{"Content-Type": []string{"application/foo.api+json"}}, `{"one":"two","three":"four"}`)
	assert.NoError(t, err)
	exp := orderedmap.New()
	exp.Set("one", "two")
	exp.Set("three", "four")
	assert.Equal(t, exp, res)
}
