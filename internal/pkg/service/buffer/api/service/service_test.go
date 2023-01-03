package service

import (
	"io"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
)

func TestParseRequestBody_Json(t *testing.T) {
	t.Parallel()

	r := io.NopCloser(strings.NewReader(`{"one":"two","three":"four"}`))
	res, err := parseRequestBody("application/json", r)
	assert.NoError(t, err)
	exp := orderedmap.New()
	exp.Set("one", "two")
	exp.Set("three", "four")
	assert.Equal(t, exp, res)
}

func TestParseRequestBody_Form(t *testing.T) {
	t.Parallel()

	r := io.NopCloser(strings.NewReader(`one=two&three=four`))
	res, err := parseRequestBody("application/x-www-form-urlencoded", r)
	assert.NoError(t, err)
	exp := orderedmap.New()
	exp.Set("one", "two")
	exp.Set("three", "four")
	assert.Equal(t, exp, res)
}

func TestParseRequestBody_CustomJsonApi(t *testing.T) {
	t.Parallel()

	r := io.NopCloser(strings.NewReader(`{"one":"two","three":"four"}`))
	res, err := parseRequestBody("application/foo.api+json", r)
	assert.NoError(t, err)
	exp := orderedmap.New()
	exp.Set("one", "two")
	exp.Set("three", "four")
	assert.Equal(t, exp, res)
}
