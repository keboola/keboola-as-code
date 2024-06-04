package reflecthelper

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
)

type testStruct struct {
	Field1 int                    `tag1:"value1" json:"field1"`
	Field2 string                 `tag1:"value1" json:"field2"`
	Field3 int                    `tag2:"value2"`
	Field4 string                 `tag2:"value3"`
	Field5 *orderedmap.OrderedMap `tag3:"value1"`
}

type testObjectWithName struct {
	id   string
	name string
}

func (v testObjectWithName) ObjectName() string {
	return v.name
}

func (v testObjectWithName) String() string {
	return v.id
}

func TestMapFromTaggedFields(t *testing.T) {
	t.Parallel()
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "field1", Value: 123},
		{Key: "field2", Value: "abc"},
	}), MapFromTaggedFields("tag1:value1", testData()))
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "Field3", Value: 456},
	}), MapFromTaggedFields("tag2:value2", testData()))
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "Field4", Value: "def"},
	}), MapFromTaggedFields("tag2:value3", testData()))
}

func TestMapFromOneTaggedField(t *testing.T) {
	t.Parallel()
	assert.Equal(t, orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "foo", Value: "bar"},
	}), MapFromOneTaggedField("tag3:value1", testData()))
}

func TestStringFromOneTaggedField(t *testing.T) {
	t.Parallel()
	value, found := StringFromOneTaggedField("tag2:value3", testData())
	assert.Equal(t, "def", value)
	assert.True(t, found)
}

func TestGetFieldsWithTag(t *testing.T) {
	t.Parallel()
	assert.Len(t, GetFieldsWithTag("tag1:value1", testData()), 2)
}

func TestGetOneFieldWithTag(t *testing.T) {
	t.Parallel()
	assert.NotNil(t, GetOneFieldWithTag("tag2:value2", testData()))
}

func TestSetFields(t *testing.T) {
	t.Parallel()
	data := testData()
	fields := GetFieldsWithTag("tag1:value1", data)
	values := orderedmap.FromPairs([]orderedmap.Pair{
		{Key: "field1", Value: 789},
		{Key: "field2", Value: "xyz"},
	})
	SetFields(fields, values, data)
	assert.Equal(t, 789, data.Field1)
	assert.Equal(t, "xyz", data.Field2)
}

func TestSetField(t *testing.T) {
	t.Parallel()
	data := testData()
	field := GetOneFieldWithTag("tag2:value2", data)
	SetField(field, 789, data)
	assert.Equal(t, 789, data.Field3)
}

func TestSortByName(t *testing.T) {
	t.Parallel()
	out := SortByName([]any{
		&testObjectWithName{id: "103", name: "C"},
		&testObjectWithName{id: "105", name: "E"},
		&testObjectWithName{id: "100", name: "A"},
		&testObjectWithName{id: "101", name: "D"},
		&testObjectWithName{id: "104", name: "B"},
		&testObjectWithName{id: "102", name: "C"},
		&testObjectWithName{id: "106", name: "E"},
	})

	assert.Equal(t, []any{
		&testObjectWithName{id: "100", name: "A"},
		&testObjectWithName{id: "104", name: "B"},
		&testObjectWithName{id: "102", name: "C"},
		&testObjectWithName{id: "103", name: "C"},
		&testObjectWithName{id: "101", name: "D"},
		&testObjectWithName{id: "105", name: "E"},
		&testObjectWithName{id: "106", name: "E"},
	}, out)
}

func testData() *testStruct {
	return &testStruct{
		Field1: 123,
		Field2: "abc",
		Field3: 456,
		Field4: "def",
		Field5: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "foo", Value: "bar"},
		}),
	}
}
