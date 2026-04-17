# Machine JSON Logging

`--machine-log-json` makes `mydumper` and `myloader` emit one JSON object per line to `stderr` or to `--logfile`.

This mode is intended for automation, CI jobs, watchdogs, and log shippers.

## Stability

The JSON contract is versioned by:

- `schema_version`
- `event_version`

Consumers should reject or explicitly handle unknown future versions.

## Guaranteed Fields

Every machine-log event contains these fields:

- `schema_version`
- `event_version`
- `seq`
- `run_id`
- `kind`
- `ts`
- `level`
- `tool`
- `program`
- `pid`
- `event`
- `phase`
- `status`
- `fatal`

`parent_seq` is also present when the current event has a causal predecessor in the same thread.

## Optional Fields

These fields are emitted only when relevant:

- `thread`
- `domain`
- `message`
- `parent_seq`
- `retryable`
- `mysql_errno`
- `error_code`
- `sqlstate`
- `object_id`
- `job_id`
- `chunk_id`

Domain-specific events may also add fields such as:

- `db`
- `table`
- `filename`
- `part_index`
- `thread_id`
- `connection_id`
- `rows_done`
- `rows_total`
- `progress_pct`
- `tables_total`
- `tables_remaining`
- `duration_ms`
- `exit_code`
- `tables`
- `files`
- `bytes`
- `errors`
- `warnings`
- `retries`
- `skipped`

Unknown extra fields must be ignored by consumers.

## Status Contract

Structured machine events use these statuses:

- `started`
- `progress`
- `finished`
- `failed`
- `cancelled`
- `warning`

## Event Types

Preferred events are domain events such as:

- `process_config`
- `process_version`
- `dump_started`
- `dump_completed`
- `restore_completed`
- `restore_sql`
- `restore_file`
- `checksum_check`

If a call site still uses a generic wrapper instead of a dedicated domain event, the fallback event name is:

- `process_notice`
- `process_warning`
- `process_error`

Those fallback events still carry the standard machine fields and may include `source_api` to indicate which wrapper emitted them.

## Sequencing

- `seq` is a process-wide monotonically increasing event number.
- `parent_seq` points to the previous causal event on the same thread when available.
- `run_id` is a UUIDv7 identifying the whole process execution.

## Completion Summary

Completion events currently include:

- `dump_completed`
- `restore_completed`

Their counters are intended for machine use and include:

- `tables`
- `files`
- `bytes`
- `errors`
- `warnings`
- `retries`
- `skipped`
- `duration_ms`
- `exit_code`

## Consumer Guidance

- Parse JSON lines incrementally.
- Key off `event`, `phase`, and `status` instead of parsing `message`.
- Treat `message` as human-readable context, not as a stable machine contract.
- Ignore unknown fields.
- Preserve `run_id`, `seq`, and `parent_seq` when forwarding events downstream.
