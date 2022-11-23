package schema

import (
	"strconv"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const recordsPrefix = Prefix("record/")

type RecordsRoot struct {
	prefix
}

type RecordsInProject struct {
	prefix
}

type RecordsInReceiver struct {
	prefix
}

type RecordsInExport struct {
	prefix
}

type RecordsInFile struct {
	prefix
}

type RecordsInSlice struct {
	prefix
}

func Records() RecordsRoot {
	return RecordsRoot{prefix: recordsPrefix}
}

func (v RecordsRoot) InProject(projectID int) RecordsInProject {
	if projectID == 0 {
		panic(errors.New("record projectID cannot be empty"))
	}
	return RecordsInProject{prefix: v.prefix.Add(strconv.Itoa(projectID))}
}

func (v RecordsInProject) InReceiver(receiverID string) RecordsInReceiver {
	if receiverID == "" {
		panic(errors.New("record receiverID cannot be empty"))
	}
	return RecordsInReceiver{prefix: v.prefix.Add(receiverID)}
}

func (v RecordsInReceiver) InExport(exportID string) RecordsInExport {
	if exportID == "" {
		panic(errors.New("record exportID cannot be empty"))
	}
	return RecordsInExport{prefix: v.prefix.Add(exportID)}
}

func (v RecordsInExport) InFile(fileID string) RecordsInFile {
	if fileID == "" {
		panic(errors.New("record fileID cannot be empty"))
	}
	return RecordsInFile{prefix: v.prefix.Add(fileID)}
}

func (v RecordsInFile) InSlice(sliceID string) RecordsInSlice {
	if sliceID == "" {
		panic(errors.New("record sliceID cannot be empty"))
	}
	return RecordsInSlice{prefix: v.prefix.Add(sliceID)}
}

func (v RecordsInSlice) ID(recordID string) Key {
	if recordID == "" {
		panic(errors.New("record recordID cannot be empty"))
	}
	return v.prefix.Key(recordID)
}
