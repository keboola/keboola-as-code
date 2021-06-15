package api

import (
	"context"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"os"
	"testing"
	"time"
)

func TestStorageApi(t *testing.T) (*StorageApi, *utils.Writer) {
	return TestStorageApiWithHost(t, utils.TestApiHost())
}

func TestStorageApiWithHost(t *testing.T, apiHost string) (*StorageApi, *utils.Writer) {
	logger, logs := utils.NewDebugLogger()
	if utils.TestIsVerbose() {
		logs.ConnectTo(os.Stdout)
	}
	a := NewStorageApi(apiHost, context.Background(), logger, false)
	a.SetRetry(3, 1*time.Millisecond, 1*time.Millisecond)
	return a, logs
}

func TestStorageApiWithToken(t *testing.T) (*StorageApi, *utils.Writer) {
	a, logs := TestStorageApiWithHost(t, utils.TestApiHost())
	token, err := a.GetToken(utils.TestTokenMaster())
	assert.NoError(t, err)
	return a.WithToken(token), logs
}
