// nolint: gocritic
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
	templatesGen "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/gen/templates"
	templatesHttp "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/http"
	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/service"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type ddLogger struct {
	*log.Logger
}

func (l ddLogger) Log(msg string) {
	l.Logger.Print(msg)
}

func main() {
	// Flags.
	httpHostF := flag.String("http-host", "0.0.0.0", "HTTP host")
	httpPortF := flag.String("http-port", "8000", "HTTP port")
	repositoriesF := flag.String("repositories", "keboola|https://github.com/keboola/keboola-as-code-templates.git|main", "Default repositories, <name1>|<repo1>|<branch1>;<name2>|<repo2>|<branch2>;...")
	debugF := flag.Bool("debug", false, "Log request and response bodies")
	flag.Parse()

	// Setup logger.
	logger := log.New(os.Stderr, "[templatesApi]", 0)

	// Envs.
	envs, err := env.FromOs()
	if err != nil {
		logger.Println("cannot load envs: " + err.Error())
		os.Exit(1)
	}

	// Start DataDog tracer.
	if envs.Get("DATADOG_ENABLED") != "false" {
		tracer.Start(
			tracer.WithServiceName("templates-api"),
			tracer.WithLogger(ddLogger{logger}),
			tracer.WithRuntimeMetrics(),
			tracer.WithAnalytics(true),
		)
		defer tracer.Stop()
	}

	// Parse repositories.
	repositories, err := parseRepositories(*repositoriesF)
	if err != nil {
		logger.Println(err.Error())
		os.Exit(1)
	}

	// Start server.
	if err := start(*httpHostF, *httpPortF, repositories, *debugF, logger, envs); err != nil {
		logger.Println(err.Error())
		os.Exit(1)
	}
}

func start(host, port string, repositories []model.TemplateRepository, debug bool, logger *log.Logger, envs *env.Map) error {
	// Create dependencies.
	d, err := dependencies.NewContainer(context.Background(), repositories, debug, logger, envs)
	if err != nil {
		return err
	}

	// Log options.
	d.Logger().Infof("starting HTTP server, host=%s, port=%s, debug=%t", host, port, debug)

	// Initialize the service.
	svc, err := service.New(d)
	if err != nil {
		return err
	}

	// Wrap the services in endpoints that can be invoked from other services
	// potentially running in different processes.
	endpoints := templatesGen.NewEndpoints(svc)

	// Create channel used by both the signal handler and server goroutines
	// to notify the main goroutine when to stop the server.
	errCh := make(chan error)

	// Setup interrupt handler. This optional step configures the process so
	// that SIGINT and SIGTERM signals cause the services to stop gracefully.
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		errCh <- fmt.Errorf("%s", <-c)
	}()

	// Create server URL.
	serverUrl := &url.URL{Scheme: "http", Host: net.JoinHostPort(host, port)}

	// Start HTTP server.
	var wg sync.WaitGroup
	templatesHttp.HandleHTTPServer(d.Ctx(), &wg, d, serverUrl, endpoints, errCh, logger, debug)

	// Wait for signal.
	logger.Printf("exiting (%v)", <-errCh)

	// Send cancellation signal to the goroutines.
	d.CtxCancelFn()()

	// Wait for goroutines.
	wg.Wait()
	logger.Println("exited")
	return nil
}

func parseRepositories(paths string) (out []model.TemplateRepository, err error) {
	paths = strings.TrimSpace(paths)
	if len(paths) == 0 {
		return nil, nil
	}

	// Definitions are separated by ";"
	for _, definition := range strings.Split(paths, ";") {
		// Definition parts are separated by "|"
		parts := strings.Split(definition, "|")
		if len(parts) < 2 {
			return nil, fmt.Errorf(`invalid repository definition "%s": required format <name>|https://<repository>|<branch> or <name>|file://<repository>`, definition)
		}
		name := parts[0]
		path := parts[1]

		switch {
		case strings.HasPrefix(path, "file://"):
			if len(parts) != 2 {
				return nil, fmt.Errorf(`invalid repository definition "%s": required format <name>|file://<repository>`, definition)
			}
			out = append(out, model.TemplateRepository{
				Type: model.RepositoryTypeDir,
				Name: name,
				Url:  strings.TrimPrefix(path, "file://"),
			})
		case strings.HasPrefix(path, "https://"):
			if len(parts) != 3 {
				return nil, fmt.Errorf(`invalid repository definition "%s": required format <name>:https://<repository>:<branch>`, definition)
			}
			if _, err = url.ParseRequestURI(path); err != nil {
				return nil, fmt.Errorf(`invalid repository url "%s": %w`, path, err)
			}
			out = append(out, model.TemplateRepository{
				Type: model.RepositoryTypeGit,
				Name: name,
				Url:  path,
				Ref:  parts[2],
			})
		default:
			return nil, fmt.Errorf(`invalid repository path "%s": must start with "file://" or "https://"`, path)
		}
	}

	return out, nil
}
