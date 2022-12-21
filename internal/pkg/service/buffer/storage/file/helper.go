package file

import (
	"fmt"
)

func sliceNumberToFilename(n int) string {
	return fmt.Sprintf("slice_%d", n)
}
