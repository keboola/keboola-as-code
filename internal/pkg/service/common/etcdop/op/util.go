package op

import (
	"bytes"
	"sort"
)

func removeOpsOverlaps(ops []TrackedOp) (out []TrackedOp) {
	// Order operations with the smallest key and with the largest range to begin
	sort.SliceStable(ops, func(i, j int) bool {
		// Sort by operation
		a, b := ops[i], ops[j]
		if a.Type != b.Type {
			return a.Type < b.Type
		}

		// Lower key first, if the key is the same, then longer range fist
		start := bytes.Compare(a.Key, b.Key)
		end := bytes.Compare(a.RangeEnd, b.RangeEnd)
		return start == -1 || start == 0 && end == 1
	})

	// Skip operations that are already covered by some previous operation
skip:
	for _, op := range ops {
		for _, compareWith := range out {
			if isOpSubsetOf(op, compareWith) {
				// The op is already covered with the compareWith operation, skip
				continue skip
			}
		}
		out = append(out, op)
	}

	return out
}

func isOpSubsetOf(op, parent TrackedOp) bool {
	// Operation type must be same
	if op.Type != parent.Type {
		// Different type
		return false
	}
	// Start must be same or greater
	if bytes.Compare(op.Key, parent.Key) == -1 {
		// Key is smaller
		return false
	}
	// End must be same or smaller
	if bytes.Compare(op.RangeEnd, parent.RangeEnd) == 1 {
		// End is greater
		return false
	}
	return true
}
