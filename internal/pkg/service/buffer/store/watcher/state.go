package watcher

import (
	"context"
	"fmt"
	"sync"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/prefixtree"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type State struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	logger log.Logger
	client *etcd.Client

	lock        *sync.RWMutex
	receivers   *objectsState[model.ReceiverBase]
	exports     *objectsState[model.ExportBase]
	mappings    *objectsState[model.Mapping]
	openedFiles *objectsState[model.File]
	tokens      *objectsState[model.Token]
}

type objectsState[T any] struct {
	*prefixtree.Tree[T]
	initDone <-chan struct{}
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	Schema() *schema.Schema
	EtcdClient() *etcd.Client
}

func NewState(d dependencies) *State {
	ctx, cancel := context.WithCancel(context.Background())
	s := &State{
		ctx:    ctx,
		wg:     &sync.WaitGroup{},
		logger: d.Logger().AddPrefix("[watcher]"),
		client: d.EtcdClient(),
		lock:   &sync.RWMutex{},
	}

	d.Process().OnShutdown(func() {
		s.logger.Info("received shutdown request")
		cancel()
		s.wg.Wait()
		s.logger.Info("shutdown done")
	})

	sm := d.Schema()
	s.receivers = watch(s, sm.Configs().Receivers().PrefixT())
	s.exports = watch(s, sm.Configs().Exports().PrefixT())
	s.mappings = watch(s, sm.Configs().Mappings().PrefixT())
	s.openedFiles = watch(s, sm.Files().Opened().PrefixT())
	s.tokens = watch(s, sm.Secrets().Tokens().PrefixT())

	// Wait for initial load
	startTime := d.Clock().Now()
	<-s.receivers.initDone
	<-s.exports.initDone
	<-s.mappings.initDone
	<-s.openedFiles.initDone
	<-s.tokens.initDone
	s.logger.Infof(`initialized | %s`, d.Clock().Since(startTime))
	return s
}

func (s *State) Receiver(k key.ReceiverKey) (out model.Receiver, found bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	out.ReceiverBase, found = s.receivers.Get(k.String())
	if !found {
		return out, false
	}

	// Add exports
	for _, base := range s.exports.AllFromPrefix(k.String()) {
		if export, found := s.exportDetails(base); found {
			out.Exports = append(out.Exports, export)
		} else {
			return out, false
		}
	}

	return out, true
}

func (s *State) exportDetails(base model.ExportBase) (out model.Export, found bool) {
	out.ExportBase = base

	out.Mapping, found = s.mappings.LastFromPrefix(base.ExportKey.String())
	if !found {
		return out, false
	}

	out.Token, found = s.tokens.LastFromPrefix(base.ExportKey.String())
	if !found {
		return out, false
	}

	out.OpenedFile, found = s.openedFiles.LastFromPrefix(base.ExportKey.String())
	if !found {
		return out, false
	}

	return out, true
}

func (s *State) onError(err error) {
	s.logger.Error(err)
}

func watch[T fmt.Stringer](s *State, prefix etcdop.PrefixT[T]) *objectsState[T] {
	watchFactory := func() (out <-chan etcdop.EventT[T], initDone <-chan struct{}) {
		return prefix.GetAllAndWatch(s.ctx, s.client, s.onError, etcd.WithCreatedNotify(), etcd.WithPrevKV())
	}

	tree := prefixtree.NewWithLock[T](s.lock)
	ch, initDone := watchFactory()

	// Log only changes, not initial load
	logsEnabled := atomic.NewBool(false)
	go func() {
		<-initDone
		logsEnabled.Store(true)
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-s.ctx.Done():
				return
			case event, ok := <-ch:
				if !ok {
					return
				}

				switch event.Type {
				case etcdop.CreateEvent, etcdop.UpdateEvent:
					k := event.Value.String()
					tree.Insert(k, event.Value)
					if logsEnabled.Load() {
						s.logger.Infof(`updated %s%s`, prefix.Prefix(), k)
					}
				case etcdop.DeleteEvent:
					if event.PrevKv == nil {
						panic("etcd.WithPrevKV() option must be used")
					}
					k := event.Value.String()
					tree.Delete(k)
					if logsEnabled.Load() {
						s.logger.Infof(`deleted %s%s`, prefix.Prefix(), k)
					}
				default:
					panic(errors.Errorf(`unexpected event type "%v"`, event.Type))
				}
			}
		}
	}()
	return &objectsState[T]{tree, initDone}
}
