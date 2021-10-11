// nolint forbidigo
package testproject

import (
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cast"
)

var projects []*Project      // nolint gochecknoglobals
var initLock = &sync.Mutex{} // nolint gochecknoglobals

func GetTestProject(t *testing.T, envs *env.Map) *Project {
	t.Helper()
	initProjects()

	if len(projects) == 0 {
		panic(fmt.Errorf(`no test project`))
	}

	for {
		// Try to find a free project
		for _, p := range projects {
			if p.tryLock(t, envs) {
				return p
			}
		}

		// No free project -> wait
		time.Sleep(50 * time.Millisecond)
	}
}

func initProjects() {
	initLock.Lock()
	defer initLock.Unlock()

	// Init only once
	if projects != nil {
		return
	}

	// Multiple test projects
	if def, found := os.LookupEnv(`TEST_KBC_PROJECTS`); found {
		// Each project definition is separated by ";"
		for _, p := range strings.Split(def, ";") {
			p = strings.TrimSpace(p)
			if len(p) == 0 {
				return
			}

			// Definition format: storage_api_host|project_id|project_token
			parts := strings.Split(p, `|`)

			// Check number of parts
			if len(parts) != 3 {
				panic(fmt.Errorf(
					`project definition in TEST_PROJECTS env must be in "storage_api_host|project_id|project_token " format, given "%s"`,
					p,
				))
			}

			host := strings.TrimSpace(parts[0])
			id := strings.TrimSpace(parts[1])
			token := strings.TrimSpace(parts[2])
			project := newProject(host, cast.ToInt(id), token)
			projects = append(projects, project)
		}
		return
	}

	// One test project
	host := os.Getenv(`TEST_KBC_STORAGE_API_HOST`)
	id := os.Getenv(`TEST_KBC_PROJECT_ID`)
	token := os.Getenv(`TEST_KBC_STORAGE_API_TOKEN`)
	if len(host) > 0 && len(id) > 0 && len(token) > 0 {
		project := newProject(host, cast.ToInt(id), token)
		projects = append(projects, project)
		return
	}

	// No test project
	panic(fmt.Errorf(`please specify one or more test projects by TEST_KBC_PROJECTS or [TEST_KBC_PROJECT_ID, TEST_KBC_STORAGE_API_HOST, TEST_KBC_STORAGE_API_TOKEN] ENVs, see "docs/DEVELOPMENT.md"`))
}
