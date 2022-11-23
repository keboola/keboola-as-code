package schema

import (
	"strconv"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
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
	return RecordsInProject{prefix: v.prefix.Add(strconv.Itoa(projectID))}
}

func (v RecordsInProject) InReceiver(receiverID string) RecordsInReceiver {
	return RecordsInReceiver{prefix: v.prefix.Add(receiverID)}
}

func (v RecordsInReceiver) InExport(exportID string) RecordsInExport {
	return RecordsInExport{prefix: v.prefix.Add(exportID)}
}

func (v RecordsInExport) InFile(fileID string) RecordsInFile {
	return RecordsInFile{prefix: v.prefix.Add(fileID)}
}

func (v RecordsInFile) InSlice(sliceID string) RecordsInSlice {
	return RecordsInSlice{prefix: v.prefix.Add(sliceID)}
}

func (v RecordsInSlice) ID(recordID string) Key {
	return v.prefix.Key(recordID)
}
