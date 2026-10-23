# Wikimedia field study

The article argues that previous-value fields in an append-only event stream can be
reconstructed at query time with LAG. Wikimedia's public `mediawiki.page_change.v1`
stream is a production example of the previous-value pattern: each event embeds a
`prior_state` block. This study captures that stream and measures how well the
article's LAG query reproduces `prior_state`, field by field, and what `prior_state`
costs in bytes and query time.

The results are in `results/wikimedia/` and in the "Wikimedia field study" section of
`results/SUMMARY.md`. They come from public Wikimedia data, not synthetic data.

## Data handling

- Source: Wikimedia EventStreams only,
  https://stream.wikimedia.org/v2/stream/mediawiki.page_change.v1.
- Etiquette: one connection at a time, and the User-Agent
  `audit-trail-bench-wikimedia-study/1.0 (https://github.com/spoorthibasu/kafka-flink-audit-trail) Python-urllib`.
  Historical reads are capped at 1,000 events per second.
- Raw events are never committed: they carry page titles, user names and edit comments.
  Keep capture and work directories outside the repository.
- Reports hold aggregates and identifiers (wiki_id, page_id, rev_id) only. No page
  content and no user names.
- Canary events (`meta.domain` = `canary`) are dropped, as Wikimedia asks.

## Files

```
sse.py              Server-Sent Events client, standard library only
probe.py            read the stream for N seconds; event rate and bytes
inspect_schema.py   schema version and prior_state fields per page_change_kind
capture.py          the capture: hourly gzip JSONL, offset resume, gap log
schema.py           derive the Avro schema from Wikimedia's published JSON schema
schema/page_change_1.12.0.avsc   the derived Avro schema
load.py             step 3: Parquet of every event; ordering key and ties
reconstruct.py      step 4: prior_state against LAG; mismatch classes
size.py             step 5: JSON and Avro bytes; Parquet bytes
distributions.py    step 6: prior_state presence and string lengths
query_cost.py       step 7: stored prior_state versus LAG
study.py            runs steps 3 to 7; writes results/wikimedia and the SUMMARY section
synthetic.py        a small synthetic capture with planted mismatches, for CI
aws/cli-policy.json least-privilege IAM policy for running a capture on EC2
```

## How the capture works

The stream holds two Kafka topics, one per Wikimedia datacenter: eqiad and codfw.
Each is a single partition. The capture reads both on one connection. Each event's
`meta` carries its topic, partition and offset.

- Resume. EventStreams gives each event an SSE id holding a Kafka timestamp plus 1 ms.
  Kafka timestamps repeat and go backwards, so resuming from that id can skip events.
  `capture.py` instead reconnects with a Last-Event-ID that holds the next offset of
  each topic it has seen. A topic it has finished resumes at offset -1, the newest, so
  the server does not resend old events.
- Gaps. Offsets within a topic are contiguous, so any jump is logged as a gap, with the
  event times on either side. The study excludes any event whose interval since its
  predecessor overlaps a gap.
- Window. `--start/--end` select by `meta.dt`. A topic is complete at its first offset
  at or after `--end`, the rule Kafka uses to find the first offset at `--start`.
  `--start earliest --end-offsets` reads from the oldest retained offset up to given
  offsets, to extend an earlier capture backwards.
- Output. `events/hour=YYYY-MM-DDTHH/<dc>.jsonl.gz` files, partitioned by `meta.dt`
  hour. Each line is `{"recv_ms", "sse_id", "event"}`, with the event exactly as
  received. Also `capture_log.jsonl` (connects, disconnects, gaps, progress) and
  `manifest.json` (window, per-topic offsets and counts, file sizes and SHA-256).
- Restarts. State is checkpointed every 30 s and on SIGTERM or SIGINT. Rerunning the
  same command resumes from the checkpoint.

EventStreams keeps between 7 and 31 days of history, per Wikimedia's documentation.
On 2026-10-23 the eqiad topic reached back to 2026-10-16T01:48Z.

## Capture again

The capture needs Python 3.9 or later and nothing else. The analysis needs
`pip install -r study/wikimedia/requirements.txt`.

Run the captures one at a time. On a Mac, `caffeinate -is` keeps the machine awake.

```bash
# 1. A window by event time, here 74 hours.
caffeinate -is python3 study/wikimedia/capture.py --start 2026-10-20T00:00:00Z \
    --end 2026-10-23T02:00:00Z --rate 1000 --out ~/audit-trail-bench-data/wikimedia/capture-2026-10-20T00Z-2026-10-23T02Z

# 2. Optional: extend it backwards to the oldest retained events. The end offsets are
#    the first offsets of the first capture (topics.*.first_offset in its manifest.json).
caffeinate -is python3 study/wikimedia/capture.py --start earliest \
    --end-offsets eqiad=1141361946,codfw=839004750 --rate 1000 \
    --out ~/audit-trail-bench-data/wikimedia/capture-earliest-to-2026-10-20T00Z

# 3. Steps 3 to 7 over both parts.
python3 study/wikimedia/study.py \
    --capture ~/audit-trail-bench-data/wikimedia/capture-earliest-to-2026-10-20T00Z \
    --capture ~/audit-trail-bench-data/wikimedia/capture-2026-10-20T00Z-2026-10-23T02Z \
    --work ~/audit-trail-bench-data/wikimedia/work --out results/wikimedia --summary results/SUMMARY.md
```

The commands above are the ones that produced the committed results. On a new capture,
use new dates and offsets: retention moves on continuously.

To look at the stream before capturing:

```bash
python3 study/wikimedia/probe.py --seconds 300 --out /some/dir/probe.jsonl
python3 study/wikimedia/inspect_schema.py --in /some/dir/probe.jsonl
```

To derive the Avro schema again (needs PyYAML):

```bash
python3 study/wikimedia/schema.py \
    --url https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/page/change/1.12.0 \
    --out study/wikimedia/schema/page_change_1.12.0.avsc
```

## Synthetic check

`make wikimedia-smoke` generates a synthetic capture (`synthetic.py`) and runs steps 3
to 7 on it into `results-smoke-wikimedia/`. It then checks the planted counts: missed
events, ordering swaps, re-keyed revisions, delete and undelete, visibility changes,
`is_redirect` omissions, editor-record drift, cross-topic pages and the gap
exclusion. CI runs the same target.

## Running the capture on EC2 instead

`aws/cli-policy.json` is an IAM policy for a CLI user that can do only the following,
all in us-east-1:

- run small t4g instances tagged `project=audit-trail-bench`
- use S3 buckets named `audit-trail-bench-*`
- pass a single instance role, `audit-trail-bench-capture`

The capture for the committed results ran on a laptop, so it did not need EC2. Never
commit account IDs, bucket names or credentials.
