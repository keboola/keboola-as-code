package storage

import "github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"

type Retryable struct {
	RetryAttempt int              `json:"retryAttempt,omitempty"`
	RetryReason  string           `json:"retryReason,omitempty" validate:"required_with=RetryAttempt,excluded_without=RetryAttempt"`
	FailedAt     *utctime.UTCTime `json:"failedAt,omitempty"  validate:"required_with=RetryAttempt,excluded_without=RetryAttempt"`
	RetryAfter   *utctime.UTCTime `json:"retryAfter,omitempty"  validate:"required_with=RetryAttempt,excluded_without=RetryAttempt,gtefield=FailedAt"`
}
