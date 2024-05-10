package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/migrate/httperror"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/migrate/request"
)

type SinkPayload struct {
	SinkID      string `json:"sinkId"`
	Type        string `json:"type"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Table       Table  `json:"table"`
}

type SinkMapping struct {
	Columns []Column `json:"columns"`
}

type Table struct {
	Type    string      `json:"type"`
	TableID string      `json:"tableId"`
	Mapping SinkMapping `json:"mapping"`
}

func (e Export) CreateSink(ctx context.Context, token string, host string) error {
	body, err := e.createSinkPayload()
	if err != nil {
		return err
	}

	path := fmt.Sprintf(sourcesPath+"/%s/sinks", e.ReceiverID)

	// Request to create sink
	resp, err := request.New(
		http.MethodPost,
		substituteHost(host, bufferPrefix, streamPrefix),
		token,
		path,
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

func (e Export) createSinkPayload() (*bytes.Buffer, error) {
	sinkPayload := &SinkPayload{
		SinkID: key.SinkID(e.ID).String(),
		Type:   definition.SinkTypeTable.String(),
		Name:   e.Name,
		Table: Table{
			Type:    definition.TableTypeKeboola.String(),
			TableID: e.Mapping.TableID,
		},
	}

	for _, m := range e.Mapping.Columns {
		var column Column
		if m.Template != nil {
			column.Template = &Template{
				Language: m.Template.Language,
				Content:  m.Template.Content,
			}
		}
		column.Name = m.Name
		column.Type = m.Type
		column.PrimaryKey = m.PrimaryKey

		sinkPayload.Table.Mapping.Columns = append(sinkPayload.Table.Mapping.Columns, column)
	}

	payloadBuf := new(bytes.Buffer)
	err := json.NewEncoder(payloadBuf).Encode(sinkPayload)
	if err != nil {
		return nil, err
	}

	return payloadBuf, nil
}
