package cliconfig_test

import (
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
)

func TestDump(t *testing.T) {
	t.Parallel()

	duration, _ := time.ParseDuration("123s")
	addrValue := netip.AddrFrom4([4]byte{1, 2, 3, 4})
	cfg := Config{
		Embedded:         Embedded{EmbeddedField: "embedded value"},
		String:           "password", // has sensitive:true tag
		Int:              123,
		Float:            4.56,
		StringWithUsage:  "value2",
		Duration:         duration,
		DurationNullable: &duration,
		URL:              &url.URL{Scheme: "http", Host: "localhost:1234"},
		Addr:             addrValue,
		Nested: Nested{
			Foo: "foo",
			Bar: 789,
		},
	}

	dump, err := cliconfig.Dump(cfg)
	assert.NoError(t, err)
	assert.Equal(t, cliconfig.KVs{
		{Key: "embedded", Value: "embedded value"},
		{Key: "int", Value: "123"},
		{Key: "float", Value: "4.56"},
		{Key: "string-with-usage", Value: "value2"},
		{Key: "duration", Value: "2m3s"},
		{Key: "duration-nullable", Value: "2m3s"},
		{Key: "url", Value: "http://localhost:1234"},
		{Key: "address", Value: "1.2.3.4"},
		{Key: "address-nullable", Value: "<nil>"},
		{Key: "nested.foo-123", Value: "foo"},
		{Key: "nested.bar", Value: "789"},
	}, dump)
	assert.Equal(t, strings.TrimSpace(`
embedded=embedded value; int=123; float=4.56; string-with-usage=value2; duration=2m3s; duration-nullable=2m3s; url=http://localhost:1234; address=1.2.3.4; address-nullable=<nil>; nested.foo-123=foo; nested.bar=789;
`), dump.String())
}
