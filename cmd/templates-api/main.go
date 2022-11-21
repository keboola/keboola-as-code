// nolint: gocritic
package main

import (
	"context"
	"flag"
	stdLog "log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/dependencies"
	templatesGen "github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/gen/templates"
	templatesHttp "github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/http"
	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/service"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ddLogger struct {
	*stdLog.Logger
}

func (l ddLogger) Log(msg string) {
	l.Logger.Print(msg)
}

func main() {
	// Flags.
	httpHostF := flag.String("http-host", "0.0.0.0", "HTTP host")
	httpPortF := flag.String("http-port", "8000", "HTTP port")
	repositoriesF := flag.String("repositories", "", "Default repositories, <name1>|<repo1>|<branch1>;<name2>|<repo2>|<branch2>;...")
	debugF := flag.Bool("debug", false, "Enable debug log level.")
	debugHttpF := flag.Bool("debug-http", false, "Log HTTP client request and response bodies.")
	flag.Parse()

	// Setup logger.
	logger := stdLog.New(os.Stderr, "[templatesApi]", 0)

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
			tracer.WithDebugMode(envs.Get("DATADOG_DEBUG") == "true"),
		)
		defer tracer.Stop()
	}

	// Parse repositories.
	var repositories []model.TemplateRepository
	if *repositoriesF == "" {
		// Default value
		repositories = []model.TemplateRepository{
			{
				Type: model.RepositoryTypeGit,
				Name: "keboola",
				Url:  "https://github.com/keboola/keboola-as-code-templates.git",
				Ref:  "main",
			},
			{
				Type: model.RepositoryTypeGit,
				Name: "keboola-beta",
				Url:  "https://github.com/keboola/keboola-as-code-templates.git",
				Ref:  "beta",
			},
			{
				Type: model.RepositoryTypeGit,
				Name: "keboola-dev",
				Url:  "https://github.com/keboola/keboola-as-code-templates.git",
				Ref:  "dev",
			},
		}
	} else {
		repositories, err = parseRepositories(*repositoriesF)
		if err != nil {
			logger.Println(err.Error())
			os.Exit(1)
		}
	}

	// Start server.
	if err := start(*httpHostF, *httpPortF, repositories, *debugF, *debugHttpF, logger, envs); err != nil {
		logger.Println(err.Error())
		os.Exit(1)
	}
}

func start(host, port string, repositories []model.TemplateRepository, debug, debugHttp bool, stdLogger *stdLog.Logger, envs *env.Map) error {
	// Create context.
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	// Create logger.
	logger := log.NewApiLogger(stdLogger, "", debug)
	logger.Infof("starting Templates API HTTP server, host=%s, port=%s, debug=%t, debug-http=%t", host, port, debug, debugHttp)

	// Create dependencies.
	d, err := dependencies.NewServerDeps(ctx, envs, logger, repositories, debug, debugHttp)
	if err != nil {
		return err
	}

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
		errCh <- errors.Errorf("%s", <-c)
	}()

	// Create server URL.
	serverUrl := &url.URL{Scheme: "http", Host: net.JoinHostPort(host, port)}

	// Start HTTP server.
	templatesHttp.HandleHTTPServer(ctx, d, serverUrl, endpoints, errCh, stdLogger, debug)

	// Wait for signal.
	logger.Infof("exiting (%v)", <-errCh)

	// Send cancellation signal to the goroutines.
	cancelFn()

	// Wait for goroutines - graceful shutdown.
	d.ServerWaitGroup().Wait()
	logger.Info("exited")
	return nil
}

func parseRepositories(paths string) (out []model.TemplateRepository, err error) {
	paths = strings.TrimSpace(paths)
	if len(paths) == 0 {
		return nil, nil
	}

	// Definitions are separated by ";"
	usedNames := make(map[string]bool)
	for _, definition := range strings.Split(paths, ";") {
		// Definition parts are separated by "|"
		parts := strings.Split(definition, "|")
		if len(parts) < 2 {
			return nil, errors.Errorf(`invalid repository definition "%s": required format <name>|https://<repository>|<branch> or <name>|file://<repository>`, definition)
		}
		name := parts[0]
		path := parts[1]

		// Each default repository must have unique name
		if usedNames[name] {
			return nil, errors.Errorf(`duplicate repository name "%s" found when parsing default repositories`, name)
		}
		usedNames[name] = true

		switch {
		case strings.HasPrefix(path, "file://"):
			if len(parts) != 2 {
				return nil, errors.Errorf(`invalid repository definition "%s": required format <name>|file://<repository>`, definition)
			}
			out = append(out, model.TemplateRepository{
				Type: model.RepositoryTypeDir,
				Name: name,
				Url:  strings.TrimPrefix(path, "file://"),
			})
		case strings.HasPrefix(path, "https://"):
			if len(parts) != 3 {
				return nil, errors.Errorf(`invalid repository definition "%s": required format <name>:https://<repository>:<branch>`, definition)
			}
			if _, err = url.ParseRequestURI(path); err != nil {
				return nil, errors.Errorf(`invalid repository url "%s": %w`, path, err)
			}
			out = append(out, model.TemplateRepository{
				Type: model.RepositoryTypeGit,
				Name: name,
				Url:  path,
				Ref:  parts[2],
			})
		default:
			return nil, errors.Errorf(`invalid repository path "%s": must start with "file://" or "https://"`, path)
		}
	}

	return out, nil
}
