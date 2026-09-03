# Job Run

**Run one or more [jobs](https://help.keboola.com/management/jobs/).**

```
kbc remote job run [branch1/]component1/config1[@tag] [branch2/]component2/config2[@tag] ... [flags]
```

If no `branch` is specified, the `main` branch is used.

If no `@tag` is specified, the default version of the component is used.

### Options

`-H, --storage-api-host <string>`
: Keboola instance URL, e.g., `connection.keboola.com`

`--timeout <string>`
: How long to wait for the job to finish (default `5m`)
  
  Specified as a sequence of decimal numbers with unit suffixes, e.g., `5m10s` or `1.5h`.  
  Available units are `ms`, `s`, `m`, and `h`.

`--async`
: Do not wait for jobs to finish (default false)

[Global Options](../../../commands.md#global-options)

### Examples

Run one configuration and wait:
```
➜ kbc remote job run ex-db-snowflake/978904392
Starting job.
Started job "328904392" using config "ex-db-snowflake/978904392"
Waiting for "328904392"
Waiting for "328904392"
Finished job "328904392"
Finished all jobs.
```

Run multiple configurations and wait; the `branch` and the component version `@tag` are specified:
```
➜ kbc remote job run keboola.ex-db-snowflake/978904392 12345/keboola.ex-db-oracle/947204232@v2.3.4 
Starting 2 jobs.
Started job "328904393" using config "keboola.ex-db-snowflake/978904392"
Started job "328904394" using config "12345/keboola.ex-db-oracle/947204232@v2.3.4"
Waiting for "328904393", "328904394"
Finished job "328904393"
Waiting for "328904394"
Waiting for "328904394"
Finished job "328904394"
Finished all jobs.
```

Run and don't wait; the `--async` flag is used:
```
➜ kbc remote job run keboola.ex-db-snowflake/978904392 keboola.ex-db-oracle/947204232 --async
Starting 2 jobs.
Started job "328904393" using config "keboola.ex-db-snowflake/978904392"
Started job "328904394" using config "keboola.ex-db-oracle/947204232"
Started all jobs.
```

## Next Steps

- [All Commands](../../../commands.md)
- [Learn more about Jobs](https://help.keboola.com/management/jobs/)
