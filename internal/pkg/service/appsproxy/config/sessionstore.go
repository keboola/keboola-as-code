package config

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	SessionStoreCookie = "cookie"
	SessionStoreRedis  = "redis"
)

type SessionStore struct {
	Type               string   `configKey:"type" configUsage:"Login session store: cookie or redis." validate:"required,oneof=cookie redis"`
	SentinelAddrs      []string `configKey:"sentinelAddrs" configUsage:"Sentinel host:port list. Required when type is redis."`
	SentinelMasterName string   `configKey:"sentinelMasterName" configUsage:"Sentinel master set name. Required when type is redis."`
	SentinelUsername   string   `configKey:"sentinelUsername" configUsage:"Sentinel ACL user. Required when type is redis."`
	SentinelPassword   string   `configKey:"sentinelPassword" configUsage:"Sentinel ACL password." sensitive:"true"`
	RedisUsername      string   `configKey:"redisUsername" configUsage:"Session store ACL user."`
	RedisPassword      string   `configKey:"redisPassword" configUsage:"Session store ACL password." sensitive:"true"`
	MigrateCookieStore bool     `configKey:"migrateCookieStore" configUsage:"When type is redis, move a valid cookie-store session into the store instead of asking for a new login."`
}

func newSessionStore() SessionStore {
	return SessionStore{
		Type:               SessionStoreCookie,
		SentinelUsername:   "sentinel",
		MigrateCookieStore: true,
	}
}

func (c *SessionStore) Validate() error {
	if c.Type != SessionStoreRedis {
		return nil
	}
	errs := errors.NewMultiError()
	if len(c.SentinelAddrs) == 0 {
		errs.Append(errors.New("sessionStore.sentinelAddrs is required when sessionStore.type is redis"))
	}
	if c.SentinelMasterName == "" {
		errs.Append(errors.New("sessionStore.sentinelMasterName is required when sessionStore.type is redis"))
	}
	if c.SentinelUsername == "" {
		errs.Append(errors.New("sessionStore.sentinelUsername is required when sessionStore.type is redis"))
	}
	return errs.ErrorOrNil()
}
