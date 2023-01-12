package apinode

import (
	"context"
	"fmt"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher/apinode/revision"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/prefixtree"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// state is a memory cached state synchronized by the etcd Watch API.
type state struct {
	logger log.Logger
	client *etcd.Client

	receivers      *stateOf[model.ReceiverBase]
	slices         *stateOf[model.Slice]
	slicesRevision *revision.Syncer
}

type stateOf[T any] struct {
	*prefixtree.TreeThreadSafe[T]
	initDone <-chan error
}

func newState(ctx context.Context, wg *sync.WaitGroup, d Dependencies) (*state, error) {
	sm := d.Schema()

	// Create
	s := &state{
		logger: d.Logger().AddPrefix("[watcher][api][state]"),
		client: d.EtcdClient(),
	}

	// Sync slices revision from API to Worker nodes
	slicesRevisionKey := sm.Runtime().APINodes().Watchers().SlicesRevision().Node(d.Process().UniqueID())
	if syncer, err := revision.NewSyncer(d, slicesRevisionKey); err == nil {
		s.slicesRevision = syncer
	} else {
		return nil, err
	}

	// Watch receivers and slices
	s.receivers = watch(ctx, wg, s, sm.Configs().Receivers().PrefixT(), nil)
	s.slices = watch(ctx, wg, s, sm.Slices().Opened().PrefixT(), s.slicesRevision)

	// Wait for initial load
	startTime := time.Now()
	errs := errors.NewMultiError()
	if err := <-s.receivers.initDone; err != nil {
		errs.Append(err)
	}
	if err := <-s.slices.initDone; err != nil {
		errs.Append(err)
	}
	if errs.Len() == 0 {
		s.logger.Infof(`initialized | %s`, time.Since(startTime))
	}
	return s, errs.ErrorOrNil()
}

func (s *state) GetReceiver(receiverKey key.ReceiverKey) (out ReceiverCore, found bool, unlockFn revision.UnlockFn) {
	unlockFn = s.slicesRevision.LockCurrentRevision()

	// Get receiver
	out.ReceiverBase, found = s.receivers.Get(receiverKey.String())
	if !found {
		unlockFn()
		return out, false, nil
	}

	// Get opened slices
	slicePerExport := make(map[key.ExportKey]bool)
	for _, slice := range s.slices.AllFromPrefix(receiverKey.String()) {
		if slicePerExport[slice.ExportKey] {
			unlockFn()
			panic(errors.Errorf(`found multiple opened slices per export "%s"`, slice.ExportKey.String()))
		}
		slicePerExport[slice.ExportKey] = true
		out.Slices = append(out.Slices, slice)
	}

	return out, true, unlockFn
}

// The function belongs to the state struct, but generic method cannot be currently defined.
func watch[T fmt.Stringer](ctx context.Context, wg *sync.WaitGroup, s *state, prefix etcdop.PrefixT[T], rev *revision.Syncer) *stateOf[T] {
	tree := prefixtree.New[T]()

	initDone := make(chan error)
	ch := prefix.GetAllAndWatch(ctx, s.client, etcd.WithCreatedNotify(), etcd.WithPrevKV())

	wg.Add(1)
	go func() {
		defer wg.Done()

		// Reset the tree on the restart event.
		reset := false

		// Log only changes, not initial load.
		logsEnabled := false

		// Channel is closed on shutdown, so the context does not have to be checked.
		for resp := range ch {
			switch {
			case resp.InitErr != nil:
				// Initialization error, stop worker via initDone channel
				initDone <- resp.InitErr
				close(initDone)
			case resp.Err != nil:
				// An error occurred, it is logged.
				// If it is a fatal error, then it is followed
				// by the "Restarted" event handled bellow,
				// and the operation starts from the beginning.
				s.logger.Error(resp.Err)
			case resp.Restarted:
				// A fatal error (etcd ErrCompacted) occurred.
				// It is not possible to continue watching, the operation must be restarted.
				reset = true
				logsEnabled = false
				s.logger.Warnf(`restart: %s`, resp.RestartReason)
			case resp.Created:
				// The watcher has been successfully created.
				// This means transition from GetAll to Watch phase.
				logsEnabled = true
				close(initDone)
			default:
				tree.ModifyAtomic(func(t *prefixtree.Tree[T]) {
					// Reset the tree after receiving the first batch after the restart.
					if reset {
						t.Reset()
						reset = false
					}

					//  Atomically process all events
					for _, event := range resp.Events {
						k := event.Value.String()
						switch event.Type {
						case etcdop.CreateEvent:
							t.Insert(k, event.Value)
							if logsEnabled {
								s.logger.Infof(`created %s%s`, prefix.Prefix(), k)
							}
						case etcdop.UpdateEvent:
							t.Insert(k, event.Value)
							if logsEnabled {
								s.logger.Infof(`updated %s%s`, prefix.Prefix(), k)
							}
						case etcdop.DeleteEvent:
							t.Delete(k)
							if logsEnabled {
								s.logger.Infof(`deleted %s%s`, prefix.Prefix(), k)
							}
						default:
							panic(errors.Errorf(`unexpected event type "%v"`, event.Type))
						}
					}
				})

				// ACK revision, so worker nodes knows that the API node is switched to the new slice.
				if rev != nil {
					rev.Notify(resp.Header.Revision)
				}
			}
		}
	}()

	return &stateOf[T]{tree, initDone}
}
