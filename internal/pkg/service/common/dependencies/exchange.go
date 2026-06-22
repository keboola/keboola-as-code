package dependencies

import (
	"context"
	"net/http"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola/management"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// IsProgrammaticToken reports whether the raw token value is a Connection
// programmatic bearer token (kbc_at_* / kbc_pat_*). It tolerates a "Bearer "
// prefix, so both raw tokens and Authorization header values match. Thin
// re-export so callers (auth handlers) don't import the SDK keboola package
// directly just for this gate.
func IsProgrammaticToken(token string) bool {
	return keboola.IsProgrammaticToken(token)
}

// ExchangeProgrammaticToken exchanges a Connection programmatic token
// (kbc_at_* / kbc_pat_*) for the target project's legacy Storage token via
// Connection's auth-bridge resolver, then builds a ProjectScope from the
// resolved token.
//
// saTokenPath is the projected Kubernetes ServiceAccount token the service
// authenticates itself with (its mapping must carry the
// internal:auth-bridge:resolve-storage-token scope); it is read per call so
// kubelet rotation needs no restart. projectID names the target project,
// because a programmatic token is not project-scoped on its own.
//
// The resolver returns the full token detail alongside the legacy token, so the
// scope is built directly from it — no redundant tokens/verify round-trip.
// Exchange failures carry the client-facing HTTP status (401/403/...) from the
// resolver's ClientStatusCode(), so the error writer maps them correctly instead
// of defaulting to 500.
func ExchangeProgrammaticToken(ctx context.Context, prjScp projectScopeDeps, saTokenPath, subjectToken string, projectID int, opts ...ProjectScopeOption) (ProjectScope, error) {
	if saTokenPath == "" {
		// The service does not accept programmatic tokens; surface a 400 rather than
		// a 500, so this client error is not logged at Error level / alerted on.
		return nil, svcerrors.WrapWithStatusCode(
			errors.New("programmatic token exchange is not configured (no service account token path)"),
			http.StatusBadRequest,
		)
	}

	// ponytail: a fresh exchanger per request — NewKeboolaServiceAccountAuth reads
	// the SA token file per call anyway, and exchange is not on the hot path.
	// Cache it on the scope if it ever shows up in a profile.
	exchanger, err := keboola.NewStorageTokenExchanger(
		connectionURL(prjScp.StorageAPIHost()),
		management.NewKeboolaServiceAccountAuth(saTokenPath),
	)
	if err != nil {
		return nil, err
	}

	res, err := exchanger.Exchange(ctx, subjectToken, projectID)
	if err != nil {
		// Map resolver failures (invalid token → 401, no project access → 403, ...)
		// to the proper HTTP status; without this they fall through to 500.
		var exErr *keboola.StorageTokenExchangeError
		if errors.As(err, &exErr) {
			return nil, svcerrors.WrapWithStatusCode(err, exErr.ClientStatusCode())
		}
		return nil, err
	}

	// Mirror the telemetry the legacy verify path sets, so programmatic-token
	// requests get the same project/token attributes in traces.
	token := *res.Token
	ctx = setProjectScopeTelemetry(ctx, token)

	return newProjectScope(ctx, prjScp, token, opts...)
}

// connectionURL turns a bare Storage API host into a resolver base URL with a
// scheme. StorageAPIHost is normalized scheme-less (see strhelper.NormalizeHost),
// so a host without a scheme defaults to https.
func connectionURL(host string) string {
	if strings.HasPrefix(host, "http://") || strings.HasPrefix(host, "https://") {
		return host
	}
	return "https://" + host
}
