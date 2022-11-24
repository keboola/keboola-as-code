package service

import (
	"io"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
)

func TestFormatUrl(t *testing.T) {
	t.Parallel()

	assert.Equal(
		t,
		"https://buffer.keboola.local/v1/import/1000/asdf/fdsa",
		formatUrl("buffer.keboola.local", 1000, "asdf", "fdsa"),
	)
}

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

func TestParseRequestBody_TooLarge(t *testing.T) {
	t.Parallel()

	r := io.NopCloser(strings.NewReader(idgenerator.Random(1000009)))
	_, err := parseRequestBody("application/x-www-form-urlencoded", r)
	assert.EqualError(t, err, "Payload too large.")
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
