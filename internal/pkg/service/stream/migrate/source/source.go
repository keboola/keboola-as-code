package source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/migrate/httperror"
)

const (
	createSourcePath    = "/v1/branches/default/sources"
	receiversBufferPath = "/v1/receivers"
	bufferPrefix        = "buffer"
	streamPrefix        = "stream"
)

type Receivers struct {
	Receivers []Receiver `json:"receivers"`
}
type Template struct {
	Language string `json:"language"`
	Content  string `json:"content"`
}
type Columns struct {
	PrimaryKey bool     `json:"primaryKey"`
	Type       string   `json:"type"`
	Name       string   `json:"name"`
	Template   Template `json:"template,omitempty"`
}
type Mapping struct {
	TableID     string    `json:"tableId"`
	Incremental bool      `json:"incremental"`
	Columns     []Columns `json:"columns"`
}
type Conditions struct {
	Count int    `json:"count"`
	Size  string `json:"size"`
	Time  string `json:"time"`
}
type Exports struct {
	ID         string     `json:"id"`
	ReceiverID string     `json:"receiverId"`
	Name       string     `json:"name"`
	Mapping    Mapping    `json:"mapping"`
	Conditions Conditions `json:"conditions"`
}
type Receiver struct {
	ID          string    `json:"id"`
	URL         string    `json:"url"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Exports     []Exports `json:"exports"`
}

type RequestConfig struct {
	method string
	host   string
	token  string
	path   string
	body   io.Reader
}

func New(method string, host string, token string, path string, body io.Reader) RequestConfig {
	return RequestConfig{
		method: method,
		host:   host,
		token:  token,
		body:   body,
		path:   path,
	}
}

func FetchBufferReceivers(ctx context.Context, host string, token string) (*Receivers, error) {
	return fetchDataFromBuffer(ctx, New(
		"GET",
		host,
		token,
		receiversBufferPath,
		nil,
	))
}

func fetchDataFromBuffer(ctx context.Context, reqConfig RequestConfig) (*Receivers, error) {
	resp, err := newHTTPRequest(ctx, reqConfig)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, httperror.Parser(resp.Body)
	}

	var result *Receivers

	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (r *Receiver) CreateSource(ctx context.Context, token string, host string) error {
	// Set a payload to create source
	body, err := r.createSourcePayload()
	if err != nil {
		return err
	}

	// Request to create source
	resp, err := newHTTPRequest(ctx, RequestConfig{
		token:  token,
		method: "POST",
		path:   createSourcePath,
		host:   substituteHost(host, bufferPrefix, streamPrefix),
		body:   body,
	})
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return httperror.Parser(resp.Body)
	}

	return nil
}

func newHTTPRequest(ctx context.Context, c RequestConfig) (*http.Response, error) {
	url := fmt.Sprintf("https://%s%s", c.host, c.path)

	request, err := http.NewRequestWithContext(ctx, c.method, url, c.body)
	if err != nil {
		return nil, err
	}

	request.Header.Add("X-StorageAPI-Token", c.token)
	request.Header.Add("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (r *Receiver) createSourcePayload() (*bytes.Buffer, error) {
	s := api.CreateSourcePayload{
		SourceID:    ptr.Ptr(key.SourceID(r.ID)),
		Type:        "http",
		Name:        r.Name,
		Description: ptr.Ptr(r.Description),
	}

	payloadBuf := new(bytes.Buffer)
	err := json.NewEncoder(payloadBuf).Encode(s)
	if err != nil {
		return nil, err
	}
	return payloadBuf, nil
}

func substituteHost(host, bufferPrefix, streamPrefix string) string {
	return strings.Replace(host, bufferPrefix, streamPrefix, 1)
}
