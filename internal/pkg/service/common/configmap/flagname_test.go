package configmap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFieldToFlagName(t *testing.T) {
	t.Parallel()

	cases := []struct{ FieldName, ExpectedFlagName string }{
		{FieldName: "", ExpectedFlagName: ""},
		{FieldName: "  ", ExpectedFlagName: ""},
		{FieldName: "foo", ExpectedFlagName: "foo"},
		{FieldName: "Foo", ExpectedFlagName: "foo"},
		{FieldName: "foo-bar", ExpectedFlagName: "foo-bar"},
		{FieldName: "fooBar", ExpectedFlagName: "foo-bar"},
		{FieldName: "FooBar", ExpectedFlagName: "foo-bar"},
		{FieldName: "---Foo---Bar---", ExpectedFlagName: "foo-bar"},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.ExpectedFlagName, fieldToFlagName(tc.FieldName))
	}
}
