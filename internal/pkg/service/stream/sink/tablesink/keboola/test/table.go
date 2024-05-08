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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func MockTableStorageAPICalls(t *testing.T, transport *httpmock.MockTransport) {
	t.Helper()
	lock := &sync.Mutex{}

	// Get table - not found
	checkedTables := make(map[keboola.TableKey]bool)
	transport.RegisterResponder(
		http.MethodGet,
		`=~/v2/storage/branch/[0-9]+/tables/[a-z0-9\.]+`,
		func(request *http.Request) (*http.Response, error) {
			lock.Lock()
			defer lock.Unlock()

			branchID, err := extractBranchIDFromURL(request.URL.String())
			if err != nil {
				return nil, err
			}

			tableID, err := extractTableIDFromURL(request.URL.String())
			if err != nil {
				return nil, err
			}

			tableKey := keboola.TableKey{BranchID: branchID, TableID: tableID}
			checkedTables[tableKey] = true
			return httpmock.NewJsonResponse(http.StatusNotFound, &keboola.StorageError{ErrCode: "storage.tables.notFound"})
		},
	)

	// Create table - ok
	jobCounter := 1000
	createTableJobs := make(map[keboola.StorageJobID]bool)
	tables := make(map[keboola.StorageJobID]keboola.TableDefinition)
	transport.RegisterResponder(
		http.MethodPost,
		`=~/v2/storage/branch/[0-9]+/buckets/[a-z0-9\.]+/tables-definition`,
		func(request *http.Request) (*http.Response, error) {
			lock.Lock()
			defer lock.Unlock()

			branchID, err := extractBranchIDFromURL(request.URL.String())
			if err != nil {
				return nil, err
			}

			bucketID, err := extractBucketIDFromURL(request.URL.String())
			if err != nil {
				return nil, err
			}

			dataBytes, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}

			data := keboola.CreateTableRequest{}
			if err := json.Decode(dataBytes, &data); err != nil {
				return nil, err
			}

			// Before POST, we expect GET request, to check bucket existence
			tableName := data.Name
			tableID := keboola.MustParseTableID(fmt.Sprintf(`%s.%s`, bucketID, tableName))
			tableKey := keboola.TableKey{BranchID: branchID, TableID: tableID}
			if !checkedTables[tableKey] {
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
