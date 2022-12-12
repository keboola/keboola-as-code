package model

import (
	"time"

	"github.com/c2h5oh/datasize"
)

type CurrentImportState struct {
	Count        int
	Size         datasize.ByteSize
	Now          time.Time
	LastImportAt time.Time
}

func DefaultConditions() ImportConditions {
	return ImportConditions{
		Count: 1000,
		Size:  1 * datasize.MB,
		Time:  5 * time.Minute,
	}
}
