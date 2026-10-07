# ADR-0012: Stream chaos, the delivery queue, and bronze files

- **Status:** Accepted
- **Date:** 2026-10-07
- **Task:** T1.8 (chaos, delivery and FileSink in T1.8a; KinesisSink in T1.8b)
- **Deviates from:** nothing in substance. SPEC §6.9's min-heap is implemented as a sorted buffer
  that behaves the same way (§3). The ADR fills in rules §6.8 and §6.9 leave open.

## Context
The spec sets these points:
- **Chaos (§6.8):** applies to both stream sources.
  - duplicates (the same `event_id` again 1 s–6 h later)
  - a delivery-lag mixture
  - malformed lines split evenly over three kinds
  - three stream incidents that are off by default (`duplicate_storm`, `silent_schema_break`,
    `late_burst`)
- **Delivery (§6.9):** a min-heap on `arrival_ts` sits in front of the sinks. Events flush as the
  clock passes their arrival, and only lagged events wait in memory.
- **`FileSink` (§6.9):** writes `bronze/<source>/yyyy=…/mm=…/dd=…/hh=…/part-<n>-<uuid>.jsonl.gz`
  by arrival hour (UTC), rolling every `stream_file_max_events`.
- **Ground truth (§6.10):** records injected duplicates, malformed and unresolvable counts, and
  expected quarantine counts.
- **Before T1.8:** producers wrote to a counting sink (ADR-0007 §5).

The spec does not say:
- what clock the queue follows in a day-stepped simulation
- how a duplicate and a malformed line relate
- which field a malformed line loses
- when an incident starts
- how chaos stays out of the simulated reality
- what the files' names and timestamps are

## Decision

### 1. Two PRs
- **T1.8a (this ADR):** the chaos layer, the delivery queue, `FileSink`, and wiring into backfill.
- **T1.8b:** `KinesisSink` with moto tests (batching, partial failures, retries). Kinesis isn't
  used before live-tail (T3.4) and the cloud run (T4.6).

### 2. The chaos layer
The layer is the producers' `EventSink`. For each event, from the `chaos` stream only:
- **Arrival:** `sent_ts` plus a lag drawn from the mixture, uniform within each band, in
  milliseconds.
- **Duplicates:** with `duplicate_rate`, one more copy with the same bytes, arriving 1 s–6 h
  after the original.
- **Malformed lines:** each delivered copy is broken with `malformed_rate`, so a duplicate can be
  broken while its twin is fine. The kind is drawn evenly:
  - **`truncated_json`:** the line is cut after a random byte. Any strict prefix of a JSON object is
    invalid.
  - **`missing_required_field`:** `event_id`, `event_type`, `event_ts`, or the entity key
    (`payload.interview_id` / `context.session_id`) is dropped.
  - **`invalid_enum`:** an enum the event carries is upper-cased (every contract value is lower
    case): the job board's `context.referrer_type`, or the first scheduling payload enum. For
    `interview_completed`, which has none, it's `event_type`.
- **Fixed draws:** ten uniforms per event, whatever is enabled. Enabling an incident therefore
  never shifts the other chaos, and a test checks that only the incident's lines change.

### 3. The delivery queue
- **Flushed one day behind:** after simulating day D, the simulation flushes with D's own UTC
  midnight as the clock.
- **Why not the next midnight:** each day's job-board traffic starts at 18:30 UTC the day before,
  when Bengaluru's local day begins. Flushing at the next midnight left 146 straggler parts at tiny.
  No event is ever stamped a whole day before its simulated day.
- **Memory:** it holds about a day of lines plus the lagged ones.
- **Late lines:** a line that does turn up for an hour already written goes into a new part there,
  like a late Firehose object.
- **Implementation:** a buffer in push order, filtered and stably sorted at each flush, so equal
  arrivals keep push order. It behaves as §6.9's heap and costs several times less in Python than a
  heap push per line.

### 4. Bronze files
- **Path:** by arrival hour (UTC), with `part-<n>` numbered per hour folder from 00000.
- **Deterministic names:** the uuid is derived from sha256(seed, source, hour, n). No random stream
  is consumed, so file layout settings can't shift the chaos.
- **Format:** orjson compact lines, one per event, each ending in `\n`.
- **Gzip:** level 1 with mtime 0. Level 1 is 2.5× faster than level 6 for files about 20%
  larger, and streams are most of bronze's bytes. HRIS stays at level 6 (ADR-0005).
- **Writing:** atomic (write a temp file beside the target, then rename). Each file's mtime is set to
  its latest arrival: deterministic, and a per-object arrival time silver can use, as an S3
  object's `LastModified`.
- **Manifest:** every part is listed (path, sha256, bytes, records), so the same-seed determinism
  check covers bronze. `--overwrite` now replaces `bronze/scheduling` and `bronze/jobboard`.

### 5. Incidents
- **Start hours:** drawn at start-up, always, in a fixed order (storm, then burst), within the
  incident's day, so the window never spills into the next day.
- **`duplicate_storm`:** job-board events whose original arrival falls in the window are duplicated
  at 0.30 (counted as `storm` duplicates).
- **`silent_schema_break`:** job-board events stamped on the incident's day have `req_id` renamed
  `requisition_id`, in place, with `schema_version` unchanged. Only that silent variant is modelled;
  `schema_version_unchanged: false` is refused at start-up.
- **`late_burst`:** scheduling events sent in the window arrive `delay_days` later than their drawn
  lag.

### 6. Chaos never changes the simulation
The layer only post-processes finished events and draws only from `chaos`. A test runs tiny with
chaos raised sharply (half the events duplicated, 5% of lines malformed, every lag under a minute)
and checks that HRIS files, the ATS snapshot, workforce events and both producers' truths are
unchanged.

### 7. Truth
Per source, counted as it happens:
- lines delivered
- duplicates (regular and storm)
- malformed lines by kind
- events lost because every copy broke, which is §13.2's "injected loss"
- lag bands
- late-burst and schema-break events
- unresolvable-timezone lines: well-formed copies with naive starts and no `timezone`, counted per
  copy, because silver quarantines each one before deduplicating

The CLI prints a `bronze:` line, and T1.10 builds `ground_truth.json` from these counts.

### 8. Memory
orjson returns bytes that keep its 4 KiB output buffer: 4,098 bytes held for a 441-byte line.
Lines wait about a day in the queue, so each is copied to its own size (9× less). At dev this
alone cut peak memory from 247 MB to 181 MB (main: 142 MB). The rest is mostly the manifest's file
entries and the day of lines in the queue.

## Results (dev, seed 1602)
- **Lines:** 1,825,768 events became 1,853,253 lines: 27,485 duplicates (1.5%), 1,821 malformed
  (0.1%), and 54,595 more than an hour late (3%).
- **Files:** 16,521 parts, 258 MB.
- **Simulation:** every number is unchanged.
- **tiny:** 3,492 parts, 262,723 lines, 255 malformed. A check reading every line found 90
  truncated, 83 invalid enums and 82 missing fields.
- **Runtime:** dev went from 43–47 s on main to 1:04–2:17 on this WSL2 machine, run back to back,
  with disk speed varying a lot from run to run. Most of the gap is filesystem metadata: one folder per hour per source, and `mkdir` alone takes about 1 ms on this
  virtual disk, 20 s for 16.5k folders. The CPU cost is about 15 s (chaos, serialization, gzip).
  T1.11 measures `full` and tunes it.

## Alternatives considered
- **Flush at the next midnight.** Rejected. It writes a straggler part for many evening hours
  (Bengaluru's early morning).
- **A heap push per line.** Rejected. It does the same work, slower in Python.
- **File names from the `chaos` stream.** Rejected. Changing `stream_file_max_events` would then
  shift every later chaos draw.
- **Draw duplicate and malformed details only when needed.** Rejected. Enabling an incident would
  then reshuffle all later chaos.
- **Malformed decided per event rather than per copy.** Rejected. A broken original and a clean
  duplicate is a real case, and silver's dedupe and quarantine must both handle it.
- **gzip level 6 for streams.** Rejected for now. It is 2.5× slower for 17% smaller files; T1.11
  can revisit it at `full`.
- **Keep orjson's bytes as they are.** Rejected. They take 9× the memory while they wait in the
  queue.

## Consequences
- **Silver (Phase 2):** gets duplicates across files, late objects, all three malformed kinds, and
  per-object arrival times (file mtime locally, `LastModified` in S3).
- **T1.10:** builds ground truth and expected quarantine counts from `ChaosTruth`.
- **T1.8b:** adds `KinesisSink` behind the same `LineSink` interface.
- **T1.11:** owns runtime at `full` (about 26k parts): gzip level, parallel writes, and GC tuning
  are the levers.

## References
- `docs/SPEC.md` §6.1, §6.2, §6.8, §6.9, §6.10, §8, §9.1, §13.2
- ADR-0003 (G3, stream transport), ADR-0005 (HRIS export), ADR-0007 §5 (sinks before T1.8)
