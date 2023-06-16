package main

import (
	"context"
	"fmt"
	dex "github.com/dexidp/dex/server"
	"github.com/dexidp/dex/storage"
	"github.com/dexidp/dex/storage/memory"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"golang.org/x/crypto/bcrypt"
	"net/http"
)

const (
	testUser     = "test@keboola.com"
	testPassword = "test"
)

func startOIDCProvider(ctx context.Context, issuer, listenAddr string, logger log.Logger, proc *servicectx.Process) error {
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(testPassword), 10)
	if err != nil {
		return err
	}

	stg := storage.WithStaticPasswords(memory.New(logger), []storage.Password{
		{
			Username: testUser,
			UserID:   "08a8684b-db88-4b73-90a9-3cd1661f5466",
			Email:    testUser,
			Hash:     passwordHash,
		},
	}, logger)

	err = stg.CreateConnector(storage.Connector{
		ID:   "local",
		Name: "Local",
		Type: dex.LocalConnector,
	})
	if err != nil {
		return err
	}

	err = stg.CreateClient(storage.Client{
		Public: true,
		ID:     "oauth2-proxy",
		Secret: "proxy",
	})
	if err != nil {
		return err
	}

	logger.Info("TMP: Starting OIDC provider ...")
	dexServer, err := dex.NewServer(ctx, dex.Config{
		Issuer:            issuer,
		AllowedOrigins:    []string{"*"},
		PasswordConnector: "local",
		Storage:           stg,
		Logger:            logger,
	})

	srv := &http.Server{Addr: listenAddr, Handler: dexServer, ReadHeaderTimeout: readHeaderTimeout}
	proc.Add(func(ctx context.Context, shutdown servicectx.ShutdownFn) {
		shutdown(srv.ListenAndServe())
	})
	proc.OnShutdown(func() {
		ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			logger.Errorf(`HTTP server shutdown error: %s`, err)
		}
	})

	return err
}

func startStaticHTTPServer(appName, listenAddr string, logger log.Logger, proc *servicectx.Process) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fmt.Sprintf("Hi from %s!\n\nURL: %s\n\nHeaders:\n", appName, req.URL.String())))
		_ = req.Header.Write(w)
	})

	logger.Infof(`TMP: Starting HTTP server %s for app "%s ...`, listenAddr, appName)
	srv := &http.Server{Addr: listenAddr, Handler: handler, ReadHeaderTimeout: readHeaderTimeout}
	proc.Add(func(ctx context.Context, shutdown servicectx.ShutdownFn) {
		shutdown(srv.ListenAndServe())
	})
	proc.OnShutdown(func() {
		ctx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			logger.Errorf(`HTTP server shutdown error: %s`, err)
		}
	})
}

func headerFromClaim(header, claim string) options.Header {
	return options.Header{
		Name: header,
		Values: []options.HeaderValue{
			{
				ClaimSource: &options.ClaimSource{
					Claim: claim,
				},
			},
		},
	}
}
