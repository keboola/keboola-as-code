// Package configstore provides database operations for configuring receivers and exports.
package configstore

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	MaxReceiversPerProject = 100
	MaxExportsPerReceiver  = 20
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

func ReceiverKey(projectID int, receiverID string) string {
	return fmt.Sprintf("config/receiver/%d/%s", projectID, receiverID)
}

func ReceiverPrefix(projectID int) string {
	return fmt.Sprintf("config/receiver/%d", projectID)
}

func ExportsPrefix(projectID int, receiverID string) string {
	return fmt.Sprintf("config/export/%d/%s/", projectID, receiverID)
}

func ExportKey(projectID int, receiverID string, exportID string) string {
	return fmt.Sprintf("config/export/%d/%s/%s", projectID, receiverID, exportID)
}

func MappingsPrefix(projectID int, receiverID string, exportID string) string {
	return fmt.Sprintf("config/mapping/revision/%d/%s/%s/", projectID, receiverID, exportID)
}

func MappingKey(projectID int, receiverID string, exportID string, revisionID int) string {
	return fmt.Sprintf("config/mapping/revision/%d/%s/%s/%08d", projectID, receiverID, exportID, revisionID)
}

type RecordKey struct {
	projectID  int
	receiverID string
	exportID   string
	fileID     string
	sliceID    string
	receivedAt time.Time
}

func (k RecordKey) String() string {
	recordID := FormatTimeForKey(k.receivedAt) + "_" + idgenerator.Random(5)
	return fmt.Sprintf("record/%d/%s/%s/%s/%s/%s", k.projectID, k.receiverID, k.exportID, k.fileID, k.sliceID, recordID)
}

func FormatTimeForKey(t time.Time) string {
	return t.Format("2006-01-02T15:04:05.000Z")
}

// TODO: use this instead of returning nil
type LimitReachedError struct {
	What string
	Max  int
}

func (e LimitReachedError) Error() string {
	return fmt.Sprintf("%s limit reached, the maximum is %d", e.What, e.Max)
}

// nolint: godox
// TODO: use this instead of returning nil from GET methods.
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

	prefix := ReceiverPrefix(receiver.ProjectID)
	logger.Debugf(`Reading "%s" count`, prefix)
	allReceivers, err := client.KV.Get(ctx, prefix, etcd.WithPrefix(), etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if allReceivers.Count >= MaxReceiversPerProject {
		return LimitReachedError{What: "receiver", Max: MaxReceiversPerProject}
	}

	key := ReceiverKey(receiver.ProjectID, receiver.ID)

	logger.Debugf(`Reading "%s" count`, key)
	receivers, err := client.KV.Get(ctx, key, etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if receivers.Count > 0 {
		return AlreadyExistsError{What: "receiver", Key: key}
	}

	logger.Debugf(`Encoding "%s"`, key)
	value, err := json.EncodeString(receiver, false)
	if err != nil {
		return err
	}

	logger.Debugf(`PUT "%s" "%s"`, key, value)
	_, err = client.KV.Put(ctx, key, value)
	if err != nil {
		return err
	}

	return nil
}

// GetReceiver fetches a receiver from the store.
//
// May fail if the receiver was not found (`NotFoundError`)
func (c *Store) GetReceiver(ctx context.Context, projectID int, receiverID string) (r *model.Receiver, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.GetReceiver")
	defer telemetry.EndSpan(span, &err)

	key := ReceiverKey(projectID, receiverID)

	logger.Debugf(`GET "%s"`, key)
	resp, err := client.KV.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	// No receiver found
	if len(resp.Kvs) == 0 {
		logger.Debugf(`No receiver "%s" found`, key)
		return nil, NotFoundError{What: "receiver", Key: key}
	}

	logger.Debugf(`Decoding "%s"`, key)
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

	prefix := ReceiverPrefix(projectID)

	logger.Debugf(`GET "%s"`, prefix)
	resp, err := client.KV.Get(ctx, prefix, etcd.WithPrefix())
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Decoding list "%s"`, prefix)
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

	key := ReceiverKey(projectID, receiverID)

	logger.Debugf(`DELETE "%s"`, key)
	r, err := client.KV.Delete(ctx, key)
	if err != nil {
		return err
	}

	if r.Deleted == 0 {
		return NotFoundError{Key: key}
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

	prefix := ExportsPrefix(projectID, receiverID)
	logger.Debugf(`Reading "%s" count`, prefix)
	receiverExports, err := client.KV.Get(ctx, prefix, etcd.WithPrefix(), etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if receiverExports.Count >= MaxExportsPerReceiver {
		return LimitReachedError{What: "export", Max: MaxExportsPerReceiver}
	}

	key := ExportKey(projectID, receiverID, export.ID)

	logger.Debugf(`Reading "%s" count`, key)
	exports, err := client.KV.Get(ctx, key, etcd.WithCountOnly())
	if err != nil {
		return err
	}
	if exports.Count > 0 {
		return AlreadyExistsError{What: "export", Key: key}
	}

	logger.Debugf(`Encoding "%s"`, key)
	value, err := json.EncodeString(export, false)
	if err != nil {
		return err
	}

	logger.Debugf(`PUT "%s" "%s"`, key, value)
	_, err = client.KV.Put(ctx, key, value)
	if err != nil {
		return err
	}

	return nil
}

func (c *Store) ListExports(ctx context.Context, projectID int, receiverID string) (r []*model.Export, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.ListExports")
	defer telemetry.EndSpan(span, &err)

	key := ExportsPrefix(projectID, receiverID)

	logger.Debugf(`GET "%s"`, key)
	resp, err := client.KV.Get(ctx, key, etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortAscend))
	if err != nil {
		return nil, err
	}

	logger.Debugf(`Decoding list "%s"`, key)
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

func (c *Store) GetCurrentMapping(ctx context.Context, projectID int, receiverID string, exportID string) (r *model.Mapping, err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.GetCurrentMapping")
	defer telemetry.EndSpan(span, &err)

	key := MappingsPrefix(projectID, receiverID, exportID)

	logger.Debugf(`GET "%s"`, key)
	// Get only the last mapping added (i.e. the one with the biggest timestamp)
	resp, err := client.KV.Get(ctx, key, etcd.WithPrefix(), etcd.WithSort(etcd.SortByKey, etcd.SortDescend), etcd.WithLimit(1))
	if err != nil {
		return nil, err
	}

	// No mapping found
	if len(resp.Kvs) == 0 {
		logger.Debugf(`No mapping "%s" found`, key)
		return nil, NotFoundError{What: "mapping", Key: key}
	}

	logger.Debugf(`Decoding "%s"`, key)
	mapping := &model.Mapping{}
	if err = json.DecodeString(string(resp.Kvs[0].Value), mapping); err != nil {
		return nil, err
	}
	return mapping, nil
}

func (c *Store) CreateRecord(ctx context.Context, recordKey RecordKey, csvData []string) (err error) {
	logger, tracer, client := c.logger, c.tracer, c.etcdClient

	_, span := tracer.Start(ctx, "keboola.go.buffer.configstore.CreateRecord")
	defer telemetry.EndSpan(span, &err)

	key := recordKey.String()

	logger.Debugf(`Encoding "%s"`, key)
	csvBuffer := new(bytes.Buffer)
	w := csv.NewWriter(csvBuffer)

	if err := w.WriteAll([][]string{csvData}); err != nil {
		return err
	}

	if err := w.Error(); err != nil {
		return err
	}

	logger.Debugf(`PUT "%s" "%s"`, key, csvBuffer.String())
	_, err = client.KV.Put(ctx, key, csvBuffer.String())
	if err != nil {
		return err
	}

	return nil
}
