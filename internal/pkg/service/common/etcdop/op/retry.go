package op

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcd "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// nolint: gochecknoglobals
var retriedErrCodes = map[codes.Code]bool{
	// Full error list: "etcd.io/etcd/api/v3/v3rpc/rpctypes/error.go"
	// Etcd sometimes doesn't make retries for mutable operations: https://github.com/etcd-io/etcd/issues/8691
	// The mutable operations are protected by the AtomicOp, so we want to do retries.
	// For example, the Unavailable error on PUT will be retried: https://github.com/etcd-io/etcd/issues/12020
	codes.Unknown:            true,
	codes.Internal:           true,
	codes.Unavailable:        true,
	codes.ResourceExhausted:  true,
	codes.DeadlineExceeded:   true,
	codes.FailedPrecondition: true,
}

// nolint: gochecknoglobals
var retriedErrs = map[error]bool{
	rpctypes.ErrFutureRev: true, // broken etcd instance may be frozen in some old revision
}

func DoWithRetry(ctx context.Context, client etcd.KV, op etcd.Op, opts ...Option) (response *RawResponse, err error) {
	// Add client and client options to the response.
	// It is used if we need to perform another database operation as part of the response processing.
	// For example, in the iterator package, to load next pages with results.
	response.Client = client
	response.Options = opts

	b := newBackoff(opts...)
	attempt := 0
	for {
		response.OpResponse, err = client.Do(ctx, op)
		if err == nil {
			break
		}

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			break
		}

		var etcdErr rpctypes.EtcdError
		if errors.As(err, &etcdErr) && !retriedErrCodes[etcdErr.Code()] && !retriedErrs[etcdErr] {
			break
		}

		attempt++
		if delay := b.NextBackOff(); delay == backoff.Stop {
			break
		} else {
			<-time.After(delay)
		}
	}

	if err != nil && attempt > 1 {
		err = errors.Errorf("%w, attempt %d, elapsed time %s, %s", err, attempt, b.GetElapsedTime(), status.Code(err))
	}

	return response, err
}

func newBackoff(opts ...Option) *backoff.ExponentialBackOff {
	c := newConfig(opts)
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.2
	b.Multiplier = 2
	b.InitialInterval = c.retryInitialInterval
	b.MaxInterval = c.retryMaxInterval
	b.MaxElapsedTime = c.retryMaxElapsedTime
	b.Reset()
	return b
}
