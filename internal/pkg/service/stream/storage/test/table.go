package test

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func MockTableStorageAPICalls(t *testing.T, branchKey key.BranchKey, transport *httpmock.MockTransport) {
	t.Helper()
	lock := &sync.Mutex{}

	// Get table - not found
	checkedTables := make(map[keboola.TableID]bool)
	transport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf(`=~/v2/storage/branch/%s/tables/[a-z\.]+`, branchKey.BranchID),
		func(request *http.Request) (*http.Response, error) {
			lock.Lock()
			defer lock.Unlock()

			parts := strings.Split(request.URL.String(), "/")
			tableID := keboola.MustParseTableID(parts[len(parts)-1])
			checkedTables[tableID] = true
			return httpmock.NewJsonResponse(http.StatusNotFound, &keboola.StorageError{ErrCode: "storage.tables.notFound"})
		},
	)

	// Create table - ok
	jobCounter := 1000
	createTableJobs := make(map[keboola.StorageJobID]bool)
	tables := make(map[keboola.StorageJobID]keboola.TableDefinition)
	transport.RegisterResponder(
		http.MethodPost,
		fmt.Sprintf(`=~/v2/storage/branch/%s/buckets/.*/tables-definition`, branchKey.BranchID),
		func(request *http.Request) (*http.Response, error) {
			lock.Lock()
			defer lock.Unlock()

			dataBytes, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}

			data := keboola.CreateTableRequest{}
			if err := json.Decode(dataBytes, &data); err != nil {
				return nil, err
			}

			// Before POST, we expect GET request, to check bucket existence
			url := strings.TrimSuffix(request.URL.String(), "/tables-definition")
			parts := strings.Split(url, "/")
			bucketID := parts[len(parts)-1]
			tableName := data.Name
			tableID := keboola.MustParseTableID(fmt.Sprintf(`%s.%s`, bucketID, tableName))
			if !checkedTables[tableID] {
				return nil, errors.Errorf(`unexpected order of requests, before creating the table "%s" via POST, it should be checked whether it exists via GET`, bucketID)
			}

			jobCounter++
			jobID := keboola.StorageJobID(jobCounter)
			createTableJobs[jobID] = true
			tables[jobID] = data.TableDefinition

			return httpmock.NewJsonResponse(http.StatusCreated, &keboola.StorageJob{
				StorageJobKey: keboola.StorageJobKey{ID: jobID},
				Status:        "processing",
			})
		},
	)

	// Create table job - ok
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/jobs/.+`,
		func(request *http.Request) (*http.Response, error) {
			lock.Lock()
			defer lock.Unlock()

			parts := strings.Split(request.URL.String(), "/")
			jobIDRaw := parts[len(parts)-1]
			jobIDInt, err := strconv.Atoi(jobIDRaw)
			if err != nil {
				return nil, errors.Errorf(`unexpected job ID "%s"`, jobIDRaw)
			}

			jobID := keboola.StorageJobID(jobIDInt)
			if !createTableJobs[jobID] {
				return nil, errors.Errorf(`job "%d" not found`, jobID)
			}

			return httpmock.NewJsonResponse(http.StatusOK, &keboola.StorageJob{
				StorageJobKey: keboola.StorageJobKey{ID: jobID},
				Status:        "success",
				Results: keboola.StorageJobResult{
					"primaryKey": tables[jobID].PrimaryKeyNames,
					"columns":    tables[jobID].Columns.Names(),
				},
			})
		},
	)
}
