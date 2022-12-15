package slicestate_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
)

func TestSTM(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var transitions []string
	stm := NewSTM(Opened, func(ctx context.Context, from, to State) error {
		transitions = append(transitions, fmt.Sprintf("%s -> %s", from, to))
		return nil
	})

	// Valid transition
	assert.NoError(t, stm.To(ctx, Closing))
	assert.Equal(t, []string{"opened -> closing"}, transitions)

	// Invalid transition
	err := stm.To(ctx, Uploaded)
	assert.Error(t, err)
	assert.Equal(t, `slice state transition "closing" -> "uploaded" is not allowed`, err.Error())
	assert.Equal(t, []string{"opened -> closing"}, transitions)
}
