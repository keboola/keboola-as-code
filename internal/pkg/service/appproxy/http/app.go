package http

import (
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
)

type DataApp struct {
	ID           AppID              `json:"id" validator:"required"`
	Name         string             `json:"name" validator:"required"`
	UpstreamHost string             `json:"upstreamUrl" validator:"required"`
	Providers    []options.Provider `json:"providers"`
}

type AppID string

func (v AppID) String() string {
	return string(v)
}
