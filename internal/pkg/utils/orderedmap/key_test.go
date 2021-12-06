package orderedmap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKey_String(t *testing.T) {
	t.Parallel()
	assert.Equal(t, ``, (Key{}).String())
	assert.Equal(t, `foo`, (Key{MapStep(`foo`)}).String())
	assert.Equal(t, `[123]`, (Key{SliceStep(123)}).String())
	assert.Equal(t, `foo1.foo2[1][2].xyz`, (Key{MapStep(`foo1`), MapStep(`foo2`), SliceStep(1), SliceStep(2), MapStep(`xyz`)}).String())
}

func TestKeyFromStr(t *testing.T) {
	t.Parallel()
	assert.Equal(t, Key{}, KeyFromStr(``))
	assert.Equal(t, Key{MapStep(`foo`)}, KeyFromStr(`foo`))
	assert.Equal(t, Key{SliceStep(123)}, KeyFromStr(`[123]`))
	assert.Equal(t, Key{MapStep(`foo1`), MapStep(`foo2`), SliceStep(1), SliceStep(2), MapStep(`xyz`)}, KeyFromStr(`foo1.foo2[1][2].xyz`))
}

func TestKey_Last(t *testing.T) {
	t.Parallel()
	assert.Equal(t, nil, (Key{}).Last())
	assert.Equal(t, MapStep(`foo`), (Key{MapStep(`foo`)}).Last())
	assert.Equal(t, SliceStep(1), (Key{SliceStep(1)}).Last())
	assert.Equal(t, MapStep(`foo2`), (Key{MapStep(`foo1`), SliceStep(1), MapStep(`foo2`)}).Last())
	assert.Equal(t, SliceStep(2), (Key{MapStep(`foo1`), SliceStep(1), MapStep(`foo2`), SliceStep(2)}).Last())
}

func TestKey_WithoutLast(t *testing.T) {
	t.Parallel()
	assert.Equal(t, Key(nil), (Key{}).WithoutLast())
	assert.Equal(t, Key{}, (Key{MapStep(`foo`)}).WithoutLast())
	assert.Equal(t, Key{}, (Key{SliceStep(1)}).WithoutLast())
	assert.Equal(t, Key{MapStep(`foo1`), SliceStep(1)}, (Key{MapStep(`foo1`), SliceStep(1), MapStep(`foo2`)}).WithoutLast())
	assert.Equal(t, Key{MapStep(`foo1`), SliceStep(1), MapStep(`foo2`)}, (Key{MapStep(`foo1`), SliceStep(1), MapStep(`foo2`), SliceStep(2)}).WithoutLast())
}
