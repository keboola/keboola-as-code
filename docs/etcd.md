# etcd

This document describes how and why we use the [etcd](https://etcd.io/docs/) database and the [etcdop](../internal/pkg/service/common/etcdop) - our high-level framework built on top of etcd.

## Motivation

### Templates API

- The Templates API currently has no database for data; it is stateless.
- Data regarding template instances is stored in the metadata of branches and configurations within the Storage API.
- A database for locks was needed to avoid collisions.
- Since the deployment of etcd was already planned for the Stream API, and it met the requirements, etcd was used for the Templates API as well.

### Stream API

- During development, it was discovered that we could not achieve the required performance with a relational database (MySQL).
- It is not so much about the number of operations per second as about the overall **architecture of the database**.
- A database was needed that would be a good core for the Stream API **distributed system**.
- We use the **[Consistent Core](https://martinfowler.com/articles/patterns-of-distributed-systems/consistent-core.html)** pattern:
  > A smaller cluster providing stronger consistency to allow the large data cluster to coordinate server activities without implementing quorum-based algorithms.
- Thus, complex questions regarding consistency, quorum, and high availability were delegated to etcd.
- etcd is used to store stream definitions and metadata about auxiliary files, slices, statistics, etc.
- The data itself is not stored in etcd, but on mounted volumes.
- etcd is also used by [Kubernetes](https://kubernetes.io/) and is therefore well-tested in production.

## Features

The following features of the etcd database are important to us:
- [Data model](https://etcd.io/docs/v3.5/learning/data_model/)
  - etcd is a [KV store](https://en.wikipedia.org/wiki/Key%E2%80%93value_database); key and value are bytes.
  - Every change in the database increments a global counter - revision.
  - The database [guarantees](https://etcd.io/docs/v3.5/learning/api_guarantees/) well-known properties: Atomicity, Consistency, Isolation, and Durability.
  - It is a database focused on consistency.
  - The maximum recommended size of all data is [8GB](https://etcd.io/docs/v3.5/dev-guide/limit/).
- Multiple keys from a `prefix/`, or `start-end` range can be loaded using [Range Queries](https://etcd.io/docs/v3.5/learning/api/#range).
- The database provides `if, then, else` [transactions](https://etcd.io/docs/v3.5/learning/api/#transaction).
  - This is a different concept than transactions in relational databases.
- The [Lease API](https://etcd.io/docs/v3.5/learning/api/#lease-api) enables deletion of keys on client disconnection or outage.
- The [Watch API](https://etcd.io/docs/v3.5/learning/api/#watch-api) is very important to us.
  - The database sends information about key changes to clients.
  - The client does not have to periodically query the database state.
  - This allows the necessary part of the database state to be stored in memory.
  - The in-memory state is updated after each change through the Watch API.
  - However, it is necessary to consider that cluster nodes will receive the update at slightly different times.
    - This must be accounted for in the system architecture.
- The [Helm chart](https://artifacthub.io/packages/helm/bitnami/etcd) is used for deployment, so we are not dependent on any cloud provider.
- etcd can be [embedded](https://pkg.go.dev/go.etcd.io/etcd/server/v3/embed) into our binaries:
  - We don't use this now, except for a few tests, but it could be useful in the future.
  - In other words, we can distribute the entire Stream API, all its components, and the database in one binary without external dependencies.

## Framework - `etcdop`

Since etcd is our main database in the Stream API, and we have complex business logic there, we needed a high-level framework on top of etcd.

Unlike relational databases, there is no ORM-like framework for etcd:
- The main reason is that it is a database for specific use in distributed systems.
- It would be overkill for a simple API, e-shop, etc.
- Kubernetes has helpers and libraries that make it easier to work with etcd.
  - However, they are internal libraries whose interface can change at any time.
  - Additionally, they don't use generic types (because they weren't available in Go at that time).
  - Generic types can make the job a lot easier.

**The `etcdop` framework provides:**
- Saving entities in `JSON` format, as etcd itself only stores bytes - see [Serde](#serde).
- `PrefixT[T]` and `KeyT[T]` generic types - abstractions for etcd prefix and key.
  - `T` is the type of the entity, see [Schema](#schema).
- A high-level wrapper for basic etcd operations, see [Basic Operations](#basic-operations).
- Combination of basic operations into a transaction, see [TxnOp](#txnop).
- Combination of basic operations and transactions into an atomic operation with multiple read phases, see [AtomicOp](#atomicop).
- Connection of a processor/callback to each operation and its parts.
- [Iterator](#iterator) loads all pages from the same revision snapshot.
- High-level Watch API streams - with manual restarts and automatic restarts on network failure, see [Watch Streams](#watch-streams).
- High-level sessions/leases - with retries on network failure, see [Session](#session).

### Packages

The `etcdop` package consists of the following sub-packages:
- [etcdop](../internal/pkg/service/common/etcdop) - prefix, key, watch stream.
- [etcdop/serde](../internal/pkg/service/common/etcdop/serde), see [Serde](#serde).
- [etcdop/op](../internal/pkg/service/common/etcdop/op) - basic operations, [TxnOp](#txnop), [AtomicOp](#atomicop).
- [etcdop/iterator](../internal/pkg/service/common/etcdop/iterator), see [Iterator](#iterator).

**In the future, it would be beneficial to combine these sub-packages into a single `etcdop` package, possibly excluding `serde`.**

### Serde

The `serde` package provides encoding, decoding, and validation operations for any value stored in etcd.

Entities are validated upon save, but also upon load.
- We use [go-playground/validator](https://github.com/go-playground/validator).
- There is also `serde.NoValidation` helper for unit tests.

The `serde.Serde` is an argument of the `etcdop.NewTypedKey` and `etcdop.NewTypedPrefix` constructors.

Supported encodings:
- Currently, only the `JSON` encoding is implemented.
- If necessary in the future, it is possible to implement additional encodings.
  - For example, for performance or database size optimization.
- Possible extensions could include `gzip` compression or binary [Protocol Buffers](https://protobuf.dev) encoding.
- It is possible to work with several encodings simultaneously:
  - This allows for maintaining backward compatibility.
  - Or implementing gradual migration.
  - For example, a header/prefix can be used to determine encoding:
    - `{` or `[` for JSON
    - `PB:` for Protocol Buffers
    - `GZIP:` for compression.
  - The disadvantage is more complicated debugging, as values will not be directly readable.

### Schema

When using the `etcdop` framework, we must start with a schema.

The schema defines:
- Prefixes:
  - `etcdop.Prefix` type is for raw values, represented by bytes.
  - `etcdop.PrefixT[T]` type is for values encoded/decoded by the [serde](#serde).
- Keys:
  - `etcdop.Key` type is for raw values, represented by bytes.
  - `etcdop.KeyT[T]` type is for values encoded/decoded by the [serde](#serde).

Example of defining a typed prefix and key:
```go
s := serde.NewJSON(validator.Validate)
pfx1 := etcdop.NewTypedPrefix[model.Slice]("storage/slice", s)
pfx2 := pfx1.Add("level").Add("local")
key1 := pfx2.Key(slice1.Key())
key2 := pfx2.Key(slice2.Key())
require.NoError(t, key1.Put(slice1).Do(ctx).Err())
require.NoError(t, key2.Put(slice2).Do(ctx).Err())
```

The above example is simple; see the `schema.go` files in the [Stream Service](../internal/pkg/service/stream) for more detailed examples.

For easier usage and more readable code, we usually define a `Schema` structure with helper methods, although this is not necessary.

### Basic Operations

The `etcdop` framework provides a high-level API for basic etcd operations such as PUT and GET.

Basic operations are defined by `op.WithResult[R]`, where `R` is the operation result type.

The `op.WithResult` structure consists of:
- `client`
  - A reference to the etcd client that will be used to execute the operation.
  - So, the `Do(ctx)` method doesn't need a `client` argument.
  - Therefore, when executing the operation in business logic code, the etcd client dependency is not needed.
  - If the operation is added to a transaction, the client from the parent transaction will be used.
- `factory`
  - `func(ctx context.Context) (etcd.Op, error)`
  - This function is a generator of the etcd client operation.
- `mapper`
  - `func(ctx context.Context, raw *RawResponse) (result R, err error)`
  - This function maps the operation result to the `R` type.
- `processors`, see [Processors](#processors).

Example of the `Put` operation:

```go
func (v Key) Put(client etcd.KV, val string, opts ...etcd.OpOption) op.WithResult[op.NoResult] {
	return op.NewForType[op.NoResult](
		client,
		// Factory
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpPut(v.Key(), val, opts...), nil
		},
		// Mapper
		func(_ context.Context, _ *RawResponse) (op.NoResult, error){
			// response is always OK
			return op.NoResult{}, nil
		},
	)
}
```

Or a more complex `PutIfNotExist`, where the result is `true` if the value has been stored:
```go
func (v Key) PutIfNotExists(client etcd.KV, val string, opts ...etcd.OpOption) op.WithResult[bool] {
	return op.NewForType[bool](
		client,
		// Factory
		func(_ context.Context) (etcd.Op, error) {
			return etcd.OpTxn(
				[]etcd.Cmp{etcd.Compare(etcd.Version(v.Key()), "=", 0)},
				[]etcd.Op{etcd.OpPut(v.Key(), val, opts...)},
				nil,
			), nil
		},
		// Mapper
		func(_ context.Context, raw *op.RawResponse) (bool, error) {
			return raw.Txn().Succeeded, nil
		},
	)
}
```

#### Retries

**`etcd` client retries:**
  - The `etcd` client performs some [retries](https://github.com/etcd-io/etcd/blob/main/client/v3/retry.go) by default.
    - By default, only retries on immutable operations are performed.
    - See [isSafeRetryImmutableRPC](https://github.com/etcd-io/etcd/blob/main/client/v3/retry.go) for more details.
- **`etcdop` framework retries:**
  - In addition to the etcd client, the `etcdop` framework also performs retries on some mutable operations.
    - For example, in our business logic, it is safe to retry a PUT operation.
    - We don't mind if the operation is performed twice; there will be two historical revisions.
    -  All operations are internally invoked by `DoWithRetry`.
    - See [retry.go](../internal/pkg/service/common/etcdop/op/retry.go) for details.
- **[AtomicOp](#atomicop) includes additional logical retries in the event of a collision.**

### Processors

Processor provides callback registration for `WithResult[R]` and `TxnOp[R]` types.

The main method is `WithProcessor`; other methods are shortcuts, for example `WithEmptyResultAsError`.

See [processor.go](../internal/pkg/service/common/etcdop/op/processor.go) for more information.

### Iterator

Due to memory consumption, it is not efficient to read a large number of keys from a prefix into memory.
It is much more efficient to process keys in batches, as they come from the database.
GC can thus free keys that have already been processed.

[Iterator](../internal/pkg/service/common/etcdop/iterator/iterator.go) abstraction:
- Reads keys from a prefix/range in pages/batches.
- The default page size is 100 keys, and there is usually no reason to change it.
- Consistency is guaranteed:
  - After loading the first page, Iterator remembers the current revision of the database.
  - Then it loads **other pages from the same revision** using the `WithRev` option.
  - This ensures that all keys are read from the same point in time.
- Native options of the etcd range queries are also available, e.g., limit or sort.

The simplest way to create an iterator is by calling the `GetAll` method on the `Prefix`/`PrefixT` structure.

The Iterator is also used internally by the `GetAllAndWatch` operation, see [Watch Streams](#watch-streams).

### TxnOp

The `etcdop` framework also provides a high-level interface for etcd `if, then, else` transactions.

Example of a simple transaction:
```go
txn := op.Txn(client)
txn.If(etcd.Compare(etcd.Value(key1.String()), "=", "foo"))
txn.Then(key1.Put(client, "foo123"))
txn.Then(key2.Put(client, "bar123"))
require.NoError(t, txn.Do(ctx).Err())
```

#### Isolation Level

The `etcdop` framework does not change the default settings of the database isolation level.

etcd provides two isolation levels on read:
- [Linearizable Isolation](https://etcd.io/docs/v3.5/learning/api_guarantees/) - requires cluster consensus.
  - > Linearizability provides the illusion that each operation applied by concurrent processes takes effect instantaneously at some point between its invocation and its response.
  - > etcd ensures linearizability for all operations by default.
  - > Linearizability comes with a cost, however, because linearized requests must go through the Raft consensus process.
- [Serializable Isolation](https://etcd.io/docs/v3.5/learning/api_guarantees/) - reads only from one node.
  - > To obtain lower latencies and higher throughput for read requests.
  - > It may access stale data with respect to quorum.

If you encounter performance issues, you can try `serializable` mode if it is acceptable for your business logic.

#### Transaction Merge

Multiple `TxnOp` instances can be joined using `ThenTxn` or `MergeTxn` methods.

**`ThenTxn`**
- Has the same semantics as the `Then` method/branch.
- The separate method is used so you have to decide which from `ThenTxn`/`MergeTxn` you want to use.
- It is rarely used.

**`MergeTxn` joins transactions in an all-or-nothing manner:**
- A common way to join transactions - perform all atomically, or nothing.
- `if` conditions are joined together.
- `then` operations are joined together.
- `else` branches are joined together, but all original `if` conditions are added.
  - This operation is implemented in the `lowLevelTxn.mergeTxn` method.
  - This algorithm is needed to determine which sub-transaction caused the entire transaction to fail.
  - Based on that, the appropriate processors/callbacks of the sub-transaction are called.
  - Example:
    - TxnA: Do `<something>` if key A exists, call a callback on failure.
    - TxnB: Do `<something>` if key B exists, call a callback on failure.
    - Txn: `TxnA.MergeTxn(TxnB)`
      - Both `if` conditions are joined together.
      - But the `else` branches must be generated to determine which sub-transaction caused the failure.

### AtomicOp

A common use-case in an API is an entity update: loading a value, modifying it in Go code, and saving the new value.
Each update must be isolated without being affected by another update.
In other words, the value in the database must not change between the read and write phases of the update operation.

To guarantee isolation, an etcd transaction must be used to save the new value of the entity only if the operation inputs have not changed since the entity was read.

```go
txn := op.Txn(client)
txn.If(etcd.Compare(etcd.ModRevision(key1.String()), "=", readRevision))
...
require.NoError(t, txn.Do(ctx).Err())
```

Writing such transactions would be complicated, especially if the transaction works with multiple keys.
Remember that etcd does not provide `BEGIN`/`COMMIT` transactions like relational databases.
For these reasons, the `etcdop` framework provides `AtomicOp`.

The [op.AtomicOp](../internal/pkg/service/common/etcdop/op/atomic.go):
- Consists of several `Read` phases and one `Write` phase at the end.
- During the `Read` phases, it automatically collects which keys have been read.
  - Keys are recorded by the [TrackerKV](../internal/pkg/service/common/etcdop/op/tracker.go).
- At the end, `if` conditions are generated/added to the `Write` phase, which check that no read key has changed.
  - See `AtomicOp.writeIfConditions` for details.
- Atomic operations can be merged together by the `AtomicOp.AddFrom` method.

**Retries:**
- If the generated `if` conditions of the Write phase are not met.
- Then, everything is retried again - all the `Read` phases and the `Write` phase.
- In this way, we get a new state and the `if` conditions will pass if nothing changes between the Read and Write phases.

**Overview of the `op.AtomicOp` methods:**
- `Read`
  - Adds a callback/factory that creates an operation for the `Read` phase.
  - The callback is executed multiple times in case of retries (!), see above.
- `Write`
  - Adds a callback/factory that creates an operation for the `Write` phase.
  - The callback is executed multiple times in case of retries (!), see above.
- `AddFrom`
  - Adds all `Read` and `Write` phases from the provided `AtomicOp`.
- `AddProcessor`, `OnResult`, ...
  - Adds a processor callback.

#### AtomicOpCtx

The [op.AtomicOpCtxFrom](../internal/pkg/service/common/etcdop/op/atomic_ctx.go) method is used to get the actual `AtomicOp` builder from the context.
This way, we can have the code divided into several packages/domains, but it can cooperate together to create one `AtomicOp`.

As mentioned, the `AtomicOp` can have multiple `Read` phases:
- This is NOT done by repeatedly calling the `Read` method.
  - The `Read` method adds additional operations to the current `Read` phase.
- A new `Read` phase is created when you add a `Read` operation in a callback of a previous `Read` operation.
- In this way, you can gradually build the `Write` phase, but it is always just one.

Example: a callback executed after loading the `model.File` entity in a `Read` phase callback.

```go
plugins.OnFileSave(func(ctx context.Context, now time.Time, original, file *model.File) error {
	if original != nil && original.State != file.State && file.State == model.FileImported {
		op.AtomicOpCtxFrom(ctx).AddFrom(r.switchSlicesToImported(*file, now))
	}
	return nil
})
```

### Watch Streams

The `etcdop` framework provides a high-level API for the etcd Watch API:
- Start with the [Prefix](../internal/pkg/service/common/etcdop/watch.go)/[PrefixT](../internal/pkg/service/common/etcdop/watch_typed.go), which provides the following methods:
  - `WatchWithoutRestart`
    - On a network failure, the stream stops and is not restarted.
  - `Watch`
    - The stream can be manually restarted and is automatically restarted on network failure.
  - `GetAllAndWatch`
    - Combines an iteration phase and then starts a watch stream.
    - Virtually, it is one stream.
    - The stream can be manually restarted and is automatically restarted on network failure.
    - Both the iteration and watch phases are then restarted.

**Stream methods overview:**
- `Channel` provides a channel with events.
- `SetupConsumer` builds a `Consumer` calling a callback for each event, which is the preferred way.

**[MirrorMap](../internal/pkg/service/common/etcdop/watch_mirror_map.go)/[MirrorTree](../internal/pkg/service/common/etcdop/watch_mirror_tree.go):**
- Both are abstractions above the stream `Consumer`.
- They allow you to create an in-memory copy of the database state, which is updated via the Watch API.
- You can store only what you need, there is a mapper function.
- `MirrorMap` requires fewer resources and can directly access one key.
- `MirrorTree` allows you to load all keys from a prefix without traversing the entire collection.
- Start with the `SetupMirrorMap`/`SetupMirrorTree` methods.

The stream event/callback contains the `restart bool` flag, and you must consider this in your business logic, as a restart can occur at any time due to a network error.

### Session

The `etcdop` framework provides a high-level API for the etcd Lease API:
- The main feature is that in the event of a network failure, the session is created again.
- Start with the [NewSessionBuilder](../internal/pkg/service/common/etcdop/session.go) method.
- The `NewMutex` method returns a `Mutex`, which combines a distributed and local lock.
  - So you can use it as a replacement for a standard `sync.Mutex`, but it acquires the lock in the entire cluster.

### Test Helpers

The `etcdhelper` package provides various utilities for unit tests:
- **Dump keys:**
  - `DumpAllKeys(ctx context.Context, client etcd.KV) (keys []string, err error)`
- **Assert keys:**
  - `AssertKeys(t assert.TestingT, client etcd.KV, expectedKeys []string, ops ...AssertOption)`
- **Dump keys and values:**
  - `DumpAll(ctx context.Context, client etcd.KV) (out []KV, err error)`
  - `DumpAllToString(ctx context.Context, client etcd.KV) (string, error)`
- **Assert keys and values:**
  - `AssertKVs(t assert.TestingT, client etcd.KV, expectedKVs []KV, ops ...AssertOption)`
  - `AssertKVsString(t assert.TestingT, client etcd.KV, expected string, ops ...AssertOption) bool`
  - `AssertKVsFromFile(t assert.TestingT, client etcd.KV, expectedKVs []KV, ops ...AssertOption) bool`
    - This is the preferred way.
    - The actual state is dumped to the `.out` directory in the test directory.
    - You can use `wildcards`, see the `wildcards` package.
