package schema

import (
	"strconv"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type records = Prefix

type RecordsRoot struct {
	records
}

type RecordsInProject struct {
	records
}

type RecordsInReceiver struct {
	records
}

type RecordsInExport struct {
	records
}

type RecordsInFile struct {
	records
}

type RecordsInSlice struct {
	records
}

func Records() RecordsRoot {
	return RecordsRoot{prefix: recordsPrefix}
}

func (v RecordsRoot) InProject(projectID int) RecordsInProject {
	if projectID == 0 {
		panic(errors.New("record projectID cannot be empty"))
	}
	return RecordsInProject{records: v.records.Add(strconv.Itoa(projectID))}
}

func (v RecordsInProject) InReceiver(receiverID string) RecordsInReceiver {
	if receiverID == "" {
		panic(errors.New("record receiverID cannot be empty"))
	}
	return RecordsInReceiver{records: v.records.Add(receiverID)}
}

func (v RecordsInReceiver) InExport(exportID string) RecordsInExport {
	if exportID == "" {
		panic(errors.New("record exportID cannot be empty"))
	}
	return RecordsInExport{records: v.records.Add(exportID)}
}

func (v RecordsInExport) InFile(fileID string) RecordsInFile {
	if fileID == "" {
		panic(errors.New("record fileID cannot be empty"))
	}
	return RecordsInFile{records: v.records.Add(fileID)}
}

func (v RecordsInFile) InSlice(sliceID string) RecordsInSlice {
	if sliceID == "" {
		panic(errors.New("record sliceID cannot be empty"))
	}
	return RecordsInSlice{records: v.records.Add(sliceID)}
}

func (v RecordsInSlice) ID(recordID string) Key {
	if recordID == "" {
		panic(errors.New("record recordID cannot be empty"))
	}
	return v.records.Key(recordID)
}
