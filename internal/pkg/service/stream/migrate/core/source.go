package core

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/migrate/httperror"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/migrate/request"
)

const (
	sourcesPath         = "/v1/branches/default/sources"
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
type Column struct {
	Type       string    `json:"type"`
	Name       string    `json:"name"`
	PrimaryKey bool      `json:"primaryKey,omitempty"`
	Template   *Template `json:"template,omitempty"`
}
type Mapping struct {
	TableID     string   `json:"tableId"`
	Incremental bool     `json:"incremental"`
	Columns     []Column `json:"columns"`
}
type Conditions struct {
	Count int    `json:"count"`
	Size  string `json:"size"`
	Time  string `json:"time"`
}
type Export struct {
	ID         string     `json:"id"`
	ReceiverID string     `json:"receiverId"`
	Name       string     `json:"name"`
	Mapping    Mapping    `json:"mapping"`
	Conditions Conditions `json:"conditions"`
}
type Receiver struct {
	ID          string   `json:"id"`
	URL         string   `json:"url"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Exports     []Export `json:"exports"`
}

func FetchBufferReceivers(ctx context.Context, host string, token string) (*Receivers, error) {
	return fetchDataFromBuffer(ctx, request.New(
		"GET",
		host,
		token,
		receiversBufferPath,
		nil,
	))
}

func fetchDataFromBuffer(ctx context.Context, reqConfig request.Config) (*Receivers, error) {
	resp, err := reqConfig.NewHTTPRequest(ctx)
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
	resp, err := request.New(
		http.MethodPost,
		substituteHost(host, bufferPrefix, streamPrefix),
		token,
		sourcesPath,
		body).
		NewHTTPRequest(ctx)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return httperror.Parser(resp.Body)
	}

	return nil
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
