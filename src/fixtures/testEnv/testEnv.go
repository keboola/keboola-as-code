package testEnv

import (
	"fmt"
	"keboola-as-code/src/utils"
	"strconv"
)

func TestApiHost() string {
	return utils.MustGetEnv("TEST_KBC_STORAGE_API_HOST")
}

func TestToken() string {
	return utils.MustGetEnv("TEST_KBC_STORAGE_API_TOKEN")
}

func TestTokenMaster() string {
	return utils.MustGetEnv("TEST_KBC_STORAGE_API_TOKEN_MASTER")
}

func TestTokenExpired() string {
	return utils.MustGetEnv("TEST_KBC_STORAGE_API_TOKEN_EXPIRED")
}

func TestProjectId() int {
	str := utils.MustGetEnv("TEST_PROJECT_ID")
	value, err := strconv.Atoi(str)
	if err != nil {
		panic(fmt.Errorf("invalid integer \"%s\": %s", str, err))
	}
	return value
}

func TestProjectName() string {
	return utils.MustGetEnv("TEST_PROJECT_NAME")
}
