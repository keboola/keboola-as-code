// Package configstore provides database operations for configuring receivers and exports.
package configstore

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	MaxImportRequestSizeInBytes = 1000000
	MaxReceiversPerProject      = 100
	MaxExportsPerReceiver       = 20
)

type Store struct {
	logger     log.Logger
	etcdClient *etcd.Client
	validator  validator.Validator
	tracer     trace.Tracer
}

func New(logger log.Logger, etcdClient *etcd.Client, validator validator.Validator, tracer trace.Tracer) *Store {
	return &Store{logger, etcdClient, validator, tracer}
}

type LimitReachedError struct {
	What string
	Max  int
}

func (e LimitReachedError) Error() string {
	return fmt.Sprintf("%s limit reached, the maximum is %d", e.What, e.Max)
}

type NotFoundError struct {
	What string
	Key  string
}

func (e NotFoundError) Error() string {
	return fmt.Sprintf("%s \"%s\" not found", e.What, e.Key)
}

type AlreadyExistsError struct {
	What string
	Key  string
}

func (e AlreadyExistsError) Error() string {
	return fmt.Sprintf(`%s "%s" already exists`, e.What, e.Key)
}

// CreateReceiver puts a receiver into the store.
//
// This method guarantees that no two receivers in the store will have the same (projectID, receiverID) pair.
//
// May fail if
// - limit is reached (`LimitReachedError`)
// - already exists (`AlreadyExistsError`)
// - validation of the model fails
// - JSON marshalling fails
// - any of the underlying ETCD calls fail.
func (c *Store) CreateReceiver(ctx context.Context, receiver model.Receiver) (err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.CreateReceiver")
	defer telemetry.EndSpan(span, &err)

	if err := c.validator.Validate(ctx, receiver); err != nil {
		return err
	}

	prefix := schema.Configs().Receivers().InProject(receiver.ProjectID)
	logger.Debugf(`Reading "%s" count`, prefix.Prefix())
	allReceivers, err := client.KV.Get(ctx, prefix.Prefix(), etcd.WithPrefix(), etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if allReceivers.Count >= MaxReceiversPerProject {
		return LimitReachedError{What: "receiver", Max: MaxReceiversPerProject}
	}

	key := prefix.ID(receiver.ID)

	logger.Debugf(`Reading "%s" count`, key.Key())
	receivers, err := client.KV.Get(ctx, key.Key(), etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if receivers.Count > 0 {
		return AlreadyExistsError{What: "receiver", Key: key.Key()}
	}

	logger.Debugf(`Encoding "%s"`, key.Key())
	value, err := json.EncodeString(receiver, false)
	if err != nil {
		return err
	}

	logger.Debugf(`PUT "%s" "%s"`, key, value)
	_, err = client.KV.Put(ctx, key.Key(), value)
	if err != nil {
		return err
	}

	return nil
}

// GetReceiver fetches a receiver from the store.
//
// May fail if the receiver was not found (`NotFoundError`).
func (c *Store) GetReceiver(ctx context.Context, projectID int, receiverID string) (r *model.Receiver, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.GetReceiver")
	defer telemetry.EndSpan(span, &err)

	key := schema.Configs().Receivers().InProject(projectID).ID(receiverID)

	logger.Debugf(`GET "%s"`, key.Key())
	resp, err := client.KV.Get(ctx, key.Key())
	if err != nil {
		return nil, err
	}

	// No receiver found
	if len(resp.Kvs) == 0 {
		logger.Debugf(`No receiver "%s" found`, key.Key())
		return nil, NotFoundError{What: "receiver", Key: key.Key()}
	}

	logger.Debugf(`Decoding "%s"`, key.Key())
	receiver := &model.Receiver{}
	if err = json.DecodeString(string(resp.Kvs[0].Value), receiver); err != nil {
		return nil, err
	}

	return receiver, nil
}

func (c *Store) ListReceivers(ctx context.Context, projectID int) (r []*model.Receiver, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.ListReceivers")
	defer telemetry.EndSpan(span, &err)

	prefix := schema.Configs().Receivers().InProject(projectID)

	logger.Debugf(`GET "%s"`, prefix.Prefix())
	resp, err := client.KV.Get(ctx, prefix.Prefix(), etcd.WithPrefix())
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Decoding list "%s"`, prefix.Prefix())
	receivers := make([]*model.Receiver, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		receiver := &model.Receiver{}
		if err = json.DecodeString(string(kv.Value), receiver); err != nil {
			return nil, err
		}
		receivers = append(receivers, receiver)
	}

	return receivers, nil
}

// DeleteReceiver deletes a receiver from the store.
//
// May fail if the receiver is not found (`NotFoundError`), or if any of the underlying ETCD calls fail.
func (c *Store) DeleteReceiver(ctx context.Context, projectID int, receiverID string) (err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.DeleteReceiver")
	defer telemetry.EndSpan(span, &err)

	key := schema.Configs().Receivers().InProject(projectID).ID(receiverID)

	logger.Debugf(`DELETE "%s"`, key.Key())
	r, err := client.KV.Delete(ctx, key.Key())
	if err != nil {
		return err
	}

	if r.Deleted == 0 {
		return NotFoundError{Key: key.Key()}
	}

	return nil
}

// CreateExport puts an export into the store.
//
// This method guarantees that no two receivers in the store will have the same (projectID, receiverID, exportID) tuple.
//
// May fail if
// - limit is reached (`LimitReachedError`)
// - already exists (`AlreadyExistsError`)
// - validation of the model fails
// - JSON marshalling fails
// - any of the underlying ETCD calls fail.
func (c *Store) CreateExport(ctx context.Context, projectID int, receiverID string, export model.Export) (err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.CreateExport")
	defer telemetry.EndSpan(span, &err)

	if err := c.validator.Validate(ctx, export); err != nil {
		return err
	}

	prefix := schema.Configs().Exports().InProject(projectID).InReceiver(receiverID)
	logger.Debugf(`Reading "%s" count`, prefix.Prefix())
	receiverExports, err := client.KV.Get(ctx, prefix.Prefix(), etcd.WithPrefix(), etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if receiverExports.Count >= MaxExportsPerReceiver {
		return LimitReachedError{What: "export", Max: MaxExportsPerReceiver}
	}

	key := prefix.ID(export.ID)

	logger.Debugf(`Reading "%s" count`, key.Key())
	exports, err := client.KV.Get(ctx, key.Key(), etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if exports.Count > 0 {
		return AlreadyExistsError{What: "export", Key: key.Key()}
	}

	logger.Debugf(`Encoding "%s"`, key.Key())
	value, err := json.EncodeString(export, false)
	if err != nil {
		return err
	}

	logger.Debugf(`PUT "%s" "%s"`, key, value)
	_, err = client.KV.Put(ctx, key.Key(), value)
	if err != nil {
		return err
	}

	return nil
}

func (c *Store) ListExports(ctx context.Context, projectID int, receiverID string) (r []*model.Export, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.ListExports")
	defer telemetry.EndSpan(span, &err)

	prefix := schema.Configs().Exports().InProject(projectID).InReceiver(receiverID)

	logger.Debugf(`GET "%s"`, prefix.Prefix())
	resp, err := client.KV.Get(ctx, prefix.Prefix(), etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Decoding list "%s"`, prefix.Prefix())
	exports := make([]*model.Export, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		export := &model.Export{}
		if err = json.DecodeString(string(kv.Value), export); err != nil {
			return nil, err
		}
		exports = append(exports, export)
	}

	return exports, nil
}

func (c *Store) CreateMapping(ctx context.Context, projectID int, receiverID string, exportID string, mapping model.Mapping) (err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.GetCurrentMapping")
	defer telemetry.EndSpan(span, &err)

	if err := c.validator.Validate(ctx, mapping); err != nil {
		return err
	}

	key := schema.Configs().Mappings().InProject(projectID).InReceiver(receiverID).InExport(exportID).Revision(mapping.RevisionID)

	logger.Debugf(`Encoding "%s"`, key.Key())
	value, err := json.EncodeString(mapping, false)
	if err != nil {
		return err
	}

	logger.Debugf(`PUT "%s" "%s"`, key, value)
	_, err = client.KV.Put(ctx, key.Key(), value)
	if err != nil {
		return err
	}

	return nil
}

func (c *Store) GetCurrentMapping(ctx context.Context, projectID int, receiverID string, exportID string) (r *model.Mapping, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.GetCurrentMapping")
	defer telemetry.EndSpan(span, &err)

	prefix := schema.Configs().Mappings().InProject(projectID).InReceiver(receiverID).InExport(exportID)

	logger.Debugf(`GET "%s"`, prefix.Prefix())
	// Get only the last mapping added (i.e. the one with the biggest timestamp)
	resp, err := client.KV.Get(ctx, prefix.Prefix(), etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortDescend), etcd.WithLimit(1))
	if err != nil {
		return nil, err
	}

	// No mapping found
	if len(resp.Kvs) == 0 {
		logger.Debugf(`No mapping "%s" found`, prefix.Prefix())
		return nil, NotFoundError{What: "mapping", Key: prefix.Prefix()}
	}

	logger.Debugf(`Decoding "%s"`, prefix.Prefix())
	mapping := &model.Mapping{}
	if err = json.DecodeString(string(resp.Kvs[0].Value), mapping); err != nil {
		return nil, err
	}
	return mapping, nil
}

func (c *Store) CreateRecord(ctx context.Context, recordKey model.RecordKey, csvData []string) (err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.CreateRecord")
	defer telemetry.EndSpan(span, &err)

	key := recordKey.Key()

	logger.Debugf(`Encoding "%s"`, key.Key())
	csvBuffer := new(bytes.Buffer)
	w := csv.NewWriter(csvBuffer)

	if err := w.WriteAll([][]string{csvData}); err != nil {
		return err
	}

	if err := w.Error(); err != nil {
		return err
	}

	logger.Debugf(`PUT "%s" "%s"`, key, csvBuffer.String())
	_, err = client.KV.Put(ctx, key.Key(), csvBuffer.String())
	if err != nil {
		return err
	}

	return nil
}
