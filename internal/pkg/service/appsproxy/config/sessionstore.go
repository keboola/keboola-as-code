package config

type SessionStore struct {
	SentinelAddrs      []string `configKey:"sentinelAddrs" configUsage:"Sentinel host:port list." validate:"required,min=1"`
	SentinelMasterName string   `configKey:"sentinelMasterName" configUsage:"Sentinel master set name." validate:"required"`
	SentinelUsername   string   `configKey:"sentinelUsername" configUsage:"Sentinel ACL user." validate:"required"`
	SentinelPassword   string   `configKey:"sentinelPassword" configUsage:"Sentinel ACL password." sensitive:"true"`
	RedisUsername      string   `configKey:"redisUsername" configUsage:"Session store ACL user."`
	RedisPassword      string   `configKey:"redisPassword" configUsage:"Session store ACL password." sensitive:"true"`
}

func newSessionStore() SessionStore {
	return SessionStore{
		SentinelUsername: "sentinel",
	}
}
