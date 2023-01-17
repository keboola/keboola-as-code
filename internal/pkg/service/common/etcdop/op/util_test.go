package op

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRemoveOverlaps(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		in, out []TrackedOp
	}{
		{
			name: "empty",
			in:   nil,
			out:  nil,
		},
		{
			name: "get op",
			in: []TrackedOp{
				{Type: GetOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
			},
			out: []TrackedOp{
				{Type: GetOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
			},
		},
		{
			name: "put op",
			in: []TrackedOp{
				{Type: PutOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
			},
			out: []TrackedOp{
				{Type: PutOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
			},
		},
		{
			name: "delete op",
			in: []TrackedOp{
				{Type: DeleteOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
			},
			out: []TrackedOp{
				{Type: DeleteOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
			},
		},
		{
			name: "get prefix op",
			in: []TrackedOp{
				{Type: GetOp, Key: []byte("some/prefix/foo005"), RangeEnd: []byte("some/prefix0"), Count: 3},
				{Type: GetOp, Key: []byte("some/prefix/foo007"), RangeEnd: []byte("some/prefix0"), Count: 1},
				{Type: GetOp, Key: []byte("some/prefix/"), RangeEnd: []byte("some/prefix0"), Count: 7},
				{Type: GetOp, Key: []byte("some/prefix/foo003"), RangeEnd: []byte("some/prefix0"), Count: 5},
				{Type: GetOp, Key: []byte("some/prefix/foo003"), RangeEnd: []byte("some/prefix0"), Count: 5},
			},
			out: []TrackedOp{
				{Type: GetOp, Key: []byte("some/prefix/"), RangeEnd: []byte("some/prefix0"), Count: 7},
			},
		},
		{
			name: "delete prefix op",
			in: []TrackedOp{
				{Type: DeleteOp, Key: []byte("some/prefix/foo005"), RangeEnd: []byte("some/prefix0"), Count: 3},
				{Type: DeleteOp, Key: []byte("some/prefix/foo007"), RangeEnd: []byte("some/prefix0"), Count: 1},
				{Type: DeleteOp, Key: []byte("some/prefix/"), RangeEnd: []byte("some/prefix0"), Count: 7},
				{Type: DeleteOp, Key: []byte("some/prefix/foo003"), RangeEnd: []byte("some/prefix0"), Count: 5},
				{Type: DeleteOp, Key: []byte("some/prefix/foo003"), RangeEnd: []byte("some/prefix0"), Count: 5},
			},
			out: []TrackedOp{
				{Type: DeleteOp, Key: []byte("some/prefix/"), RangeEnd: []byte("some/prefix0"), Count: 7},
			},
		},
		{
			name: "complex",
			in: []TrackedOp{
				{Type: DeleteOp, Key: []byte("some/prefix/foo005"), RangeEnd: []byte("some/prefix0"), Count: 3},
				{Type: GetOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
				{Type: DeleteOp, Key: []byte("some/prefix/foo007"), RangeEnd: []byte("some/prefix0"), Count: 1},
				{Type: GetOp, Key: []byte("some/prefix/foo005"), RangeEnd: []byte("some/prefix0"), Count: 3},
				{Type: GetOp, Key: []byte("some/prefix/foo007"), RangeEnd: []byte("some/prefix0"), Count: 1},
				{Type: DeleteOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
				{Type: GetOp, Key: []byte("some/prefix/"), RangeEnd: []byte("some/prefix0"), Count: 7},
				{Type: DeleteOp, Key: []byte("some/prefix/foo003"), RangeEnd: []byte("some/prefix0"), Count: 5},
				{Type: GetOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
				{Type: PutOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
				{Type: DeleteOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
				{Type: GetOp, Key: []byte("some/prefix/foo003"), RangeEnd: []byte("some/prefix0"), Count: 5},
				{Type: PutOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
				{Type: DeleteOp, Key: []byte("some/prefix/"), RangeEnd: []byte("some/prefix0"), Count: 7},
			},
			out: []TrackedOp{
				{Type: GetOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
				{Type: GetOp, Key: []byte("some/prefix/"), RangeEnd: []byte("some/prefix0"), Count: 7},
				{Type: PutOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
				{Type: DeleteOp, Count: 1, Key: []byte("key"), RangeEnd: nil},
				{Type: DeleteOp, Key: []byte("some/prefix/"), RangeEnd: []byte("some/prefix0"), Count: 7},
			},
		},
	}

	for _, tc := range testCases {
		assert.Equal(t, tc.out, removeOpsOverlaps(tc.in), fmt.Sprintf("test case %s", tc.name))
	}
}
