#!/usr/bin/env python3
"""ERC-721 Transfer events: the embedded previous owner (from) against LAG(to).

Every ERC-721 Transfer event carries `from`, the token's previous owner, beside `to`, the
new owner: a previous-value field in an append-only stream. Within one token (contract
address and tokenId), ordered by block number and log index, LAG(to) should reproduce
`from` whenever the token's previous transfer is in the window. This script measures how
often it does on the AWS Public Blockchain Data (s3://aws-public-blockchain/v1.0/eth/logs/,
one Parquet file per day, read anonymously over HTTPS; no AWS account).

  download      fetch the window's daily logs files into --raw-dir (outside the repository)
  analyze       select ERC-721 Transfer events with DuckDB, compare LAG(to) with from,
                classify every mismatch, write aggregates to --out
  chain-check   check a random sample of mismatches against an Ethereum JSON-RPC endpoint
  summary       write the SUMMARY.md section from the aggregates

An ERC-721 Transfer event has topic0 = keccak256("Transfer(address,address,uint256)") =
0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef and exactly four
topics: from, to and tokenId, all indexed. ERC-20's Transfer has the same topic0 and three
topics. Within a token, transfers are ordered by block_number, then log_index (which
Ethereum numbers per block); the script checks that this pair is unique and that log_index
order agrees with transaction_index order. Mints (from is the zero address) have no
predecessor: they are not compared, but they serve as predecessors.

Mismatch classes, checked in this order:
  same_block_ordering             from is the `to` of another transfer of the token in the
                                  same block
  non_standard_contract           the contract also emits three-topic (ERC-20 style)
                                  Transfer events in the window, so it mixes ERC-20 and
                                  ERC-721 semantics under one event signature
  previous_transfer_not_captured  from is never the `to` of a captured transfer of the token
                                  in the window: the token reached `from` through no
                                  ERC-721 Transfer event in the window
  other                           anything else (from is the `to` of an earlier, not
                                  immediate, transfer of the token in another block)

Chain check, per sampled mismatch (or matched pair, with --population matches): the
token's Transfer events (contract address, topic0, tokenId) from the predecessor's block to
the transfer's block, from Etherscan's API V2 logs module, compared with the dataset's
events for that token in that range; and eth_call supportsInterface (0x80ac58cd, ERC-721
under ERC-165) on the contract at the latest block, through the same API's proxy module.
Keyless public endpoints refuse or cap eth_getLogs over ranges of thousands of blocks, so
the check needs an Etherscan API key in the environment variable ETHERSCAN_API_KEY. The
key is never printed, logged or written. With --pause 1, the check makes at most one call
per second, under the free tier's limit of 3 calls per second.

Only aggregates reach --out: counts and shares. Contract addresses, token ids, wallet
addresses and transaction hashes stay in --work, outside the repository.

  python3 study/ethereum/erc721_from.py download --start 2026-10-24 --days 3 --raw-dir <dir>
  python3 study/ethereum/erc721_from.py analyze --start 2026-10-24 --days 3 --raw-dir <dir> --work <dir> --out results/ethereum
  python3 study/ethereum/erc721_from.py chain-check --work <dir> --out results/ethereum --sample 50 --seed 20261028
  python3 study/ethereum/erc721_from.py chain-check --population matches --work <dir> --out results/ethereum
  python3 study/ethereum/erc721_from.py summary --out results/ethereum --summary results/SUMMARY.md
"""
import argparse
import collections
import csv
import datetime
import hashlib
import json
import os
import random
import subprocess
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from fractions import Fraction

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "bench"))
from summary_sections import replace_section, exact_or_prefix  # noqa: E402

BUCKET_URL = "https://aws-public-blockchain.s3.us-east-2.amazonaws.com/"
PREFIX = "v1.0/eth/logs/date=%s/"
TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_TOPIC = "0x" + "0" * 64
ERC721_INTERFACE = "0x80ac58cd"
EXPLORER = "https://api.etherscan.io/v2/api"
CHAIN_ID = 1
KEY_ENV = "ETHERSCAN_API_KEY"
PAGE = 1000  # the logs module's largest page
SECTION_HEADING = "## Ethereum field check: ERC-721 Transfer from against LAG(to)"
USER_AGENT = "audit-trail-bench-erc721-check/1.0 (https://github.com/spoorthibasu/kafka-flink-audit-trail)"
CLASSES = ("same_block_ordering", "non_standard_contract", "previous_transfer_not_captured", "other")


def days(start, n):
    d = datetime.date.fromisoformat(start)
    return [(d + datetime.timedelta(days=i)).isoformat() for i in range(n)]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path, rows):
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def get(url, timeout=600):
    return urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": USER_AGENT}), timeout=timeout)


# ---------------------------------------------------------------------- download


def list_objects(day):
    q = urllib.parse.urlencode({"list-type": "2", "prefix": PREFIX % day})
    root = ET.fromstring(get(BUCKET_URL + "?" + q, 60).read())
    ns = {"s3": root.tag.split("}")[0].strip("{")}
    return [(c.find("s3:Key", ns).text, int(c.find("s3:Size", ns).text)) for c in root.findall("s3:Contents", ns)]


def download(a):
    os.makedirs(a.raw_dir, exist_ok=True)
    total = 0
    for day in days(a.start, a.days):
        for key, size in list_objects(day):
            path = os.path.join(a.raw_dir, "date=%s" % day, os.path.basename(key))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if not (os.path.exists(path) and os.path.getsize(path) == size):
                with get(BUCKET_URL + urllib.parse.quote(key)) as r, open(path + ".part", "wb") as fh:
                    while True:
                        chunk = r.read(1 << 22)
                        if not chunk:
                            break
                        fh.write(chunk)
                os.replace(path + ".part", path)
            if os.path.getsize(path) != size:
                raise RuntimeError("%s: %d bytes, listing says %d" % (path, os.path.getsize(path), size))
            total += size
            print("%s %s %d bytes" % (day, os.path.basename(key), size), flush=True)
    print("%d bytes" % total)


# ----------------------------------------------------------------------- analyze


def analyze(a):
    files = []
    for day in days(a.start, a.days):
        d = os.path.join(a.raw_dir, "date=%s" % day)
        found = sorted(os.path.join(d, f) for f in os.listdir(d) if f.endswith(".parquet")) if os.path.isdir(d) else []
        if not found:
            raise SystemExit("no files for %s; run download first" % day)
        files += found
    os.makedirs(a.work, exist_ok=True)
    os.makedirs(a.out, exist_ok=True)
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    con = duckdb.connect(os.path.join(a.work, "transfers.duckdb"))
    src = "read_parquet([%s])" % ", ".join("'%s'" % f for f in files)
    q = lambda sql: con.execute(sql).fetchone()  # noqa: E731
    logs, first_block, last_block = q("SELECT count(*), min(block_number), max(block_number) FROM %s" % src)
    con.execute("CREATE OR REPLACE TABLE t721 AS SELECT block_number, transaction_index, log_index, transaction_hash, "
                "lower(address) AS contract, lower(topics[2]) AS from_topic, lower(topics[3]) AS to_topic, "
                "lower(topics[4]) AS token_topic FROM %s WHERE len(topics) = 4 AND lower(topics[1]) = '%s'" % (src, TRANSFER))
    con.execute("CREATE OR REPLACE TABLE t20_contracts AS SELECT DISTINCT lower(address) AS contract FROM %s "
                "WHERE len(topics) = 3 AND lower(topics[1]) = '%s'" % (src, TRANSFER))
    raw, dup_keys = q("SELECT count(*), count(*) - count(DISTINCT (block_number, log_index)) FROM t721")
    log_key_dups = q("SELECT count(*) - count(DISTINCT (block_number, log_index)) FROM %s" % src)[0]
    order_disagree = q("SELECT count(*) FROM (SELECT transaction_index, LAG(transaction_index) OVER "
                       "(PARTITION BY block_number ORDER BY log_index) AS p FROM %s) WHERE p > transaction_index" % src)[0]
    con.execute("CREATE OR REPLACE TABLE t AS SELECT * FROM t721 QUALIFY row_number() OVER "
                "(PARTITION BY block_number, log_index ORDER BY transaction_hash) = 1")
    transfers, contracts, tokens, mints, burns = q(
        "SELECT count(*), count(DISTINCT contract), count(DISTINCT (contract, token_topic)), "
        "count(*) FILTER (WHERE from_topic = '%s'), count(*) FILTER (WHERE to_topic = '%s') FROM t" % (ZERO_TOPIC, ZERO_TOPIC))
    con.execute("CREATE OR REPLACE TABLE o AS SELECT *, LAG(to_topic) OVER w AS prev_to, LAG(block_number) OVER w AS prev_block, "
                "LAG(log_index) OVER w AS prev_log_index FROM t "
                "WINDOW w AS (PARTITION BY contract, token_topic ORDER BY block_number, log_index)")
    with_pred, compared, matches = q(
        "SELECT count(*) FILTER (WHERE prev_to IS NOT NULL), "
        "count(*) FILTER (WHERE prev_to IS NOT NULL AND from_topic <> '%s'), "
        "count(*) FILTER (WHERE prev_to IS NOT NULL AND from_topic <> '%s' AND from_topic = prev_to) FROM o" % (ZERO_TOPIC, ZERO_TOPIC))
    con.execute("""
        CREATE OR REPLACE TABLE mismatch AS
        WITH m AS (SELECT * FROM o WHERE prev_to IS NOT NULL AND from_topic <> '%(z)s' AND from_topic <> prev_to)
        SELECT m.*,
          CASE
            WHEN EXISTS (SELECT 1 FROM t x WHERE x.contract = m.contract AND x.token_topic = m.token_topic
                         AND x.block_number = m.block_number AND x.log_index <> m.log_index AND x.to_topic = m.from_topic)
              THEN 'same_block_ordering'
            WHEN m.contract IN (SELECT contract FROM t20_contracts) THEN 'non_standard_contract'
            WHEN NOT EXISTS (SELECT 1 FROM t x WHERE x.contract = m.contract AND x.token_topic = m.token_topic
                             AND x.to_topic = m.from_topic) THEN 'previous_transfer_not_captured'
            ELSE 'other'
          END AS class
        FROM m""" % {"z": ZERO_TOPIC})
    classes = dict(con.execute("SELECT class, count(*) FROM mismatch GROUP BY class").fetchall())
    mismatches = compared - matches
    if sum(classes.values()) != mismatches:
        raise RuntimeError("classified %d of %d mismatches" % (sum(classes.values()), mismatches))
    # Control: the same comparison on a copy in which one compared transfer's from is replaced
    # by the zero-padded topic of another address must report exactly one more mismatch.
    planted = q("SELECT block_number, log_index FROM o WHERE prev_to IS NOT NULL AND from_topic <> '%s' "
                "AND from_topic = prev_to ORDER BY block_number, log_index LIMIT 1" % ZERO_TOPIC)
    control_mismatches = None
    if planted:
        control_mismatches = q(
            "SELECT count(*) FILTER (WHERE prev_to IS NOT NULL AND from_topic <> '%s' AND from_topic <> prev_to) FROM "
            "(SELECT *, LAG(to_topic) OVER (PARTITION BY contract, token_topic ORDER BY block_number, log_index) AS prev_to "
            "FROM (SELECT * REPLACE (CASE WHEN block_number = %d AND log_index = %d THEN '0x%s' ELSE from_topic END AS from_topic) "
            "FROM t))" % (ZERO_TOPIC, planted[0], planted[1], "0" * 24 + "c0" * 20))[0]
    top10 = q("SELECT coalesce(sum(n), 0) FROM (SELECT count(*) AS n FROM mismatch GROUP BY contract ORDER BY n DESC LIMIT 10)")[0]
    mismatch_contracts = q("SELECT count(DISTINCT contract) FROM mismatch")[0]
    hybrid_contracts = q("SELECT count(*) FROM (SELECT DISTINCT contract FROM t) WHERE contract IN (SELECT contract FROM t20_contracts)")[0]
    con.execute("COPY (SELECT contract, token_topic, block_number, log_index, prev_block, prev_log_index, from_topic, prev_to, class "
                "FROM mismatch ORDER BY block_number, log_index) TO '%s' (FORMAT PARQUET)" % os.path.join(a.work, "mismatches.parquet"))
    con.execute("COPY (SELECT contract, token_topic, block_number, log_index FROM t) TO '%s' (FORMAT PARQUET)"
                % os.path.join(a.work, "transfers.parquet"))
    con.execute("COPY (SELECT contract, token_topic, block_number, log_index, prev_block, 'match' AS class FROM o "
                "WHERE prev_to IS NOT NULL AND from_topic <> '%s' AND from_topic = prev_to ORDER BY block_number, log_index) "
                "TO '%s' (FORMAT PARQUET)" % (ZERO_TOPIC, os.path.join(a.work, "matches.parquet")))
    per_day = []
    for day, f in zip(days(a.start, a.days), files):
        n, n721 = q("SELECT count(*), count(*) FILTER (WHERE len(topics) = 4 AND lower(topics[1]) = '%s') FROM read_parquet('%s')"
                    % (TRANSFER, f))
        per_day.append({"date": day, "file_bytes": os.path.getsize(f), "logs": n, "erc721_transfer_events": n721})
    con.close()

    rate = Fraction(matches, compared) if compared else Fraction(0)
    summary = collections.OrderedDict([
        ("window_start", a.start), ("days", a.days), ("files", len(files)), ("file_bytes", sum(os.path.getsize(f) for f in files)),
        ("logs", logs), ("first_block", first_block), ("last_block", last_block),
        ("log_block_log_index_repeats", log_key_dups), ("log_index_order_against_transaction_index", order_disagree),
        ("erc721_transfer_events", raw), ("erc721_repeated_block_log_index", dup_keys), ("transfers", transfers),
        ("contracts", contracts), ("tokens", tokens), ("mints_from_zero", mints), ("burns_to_zero", burns),
        ("contracts_also_emitting_erc20_style_transfer", hybrid_contracts),
        ("transfers_with_predecessor", with_pred), ("compared_excluding_mints", compared),
        ("from_equals_lag_to", matches), ("mismatches", mismatches), ("match_rate", exact_or_prefix(rate, 6)),
        ("contracts_with_mismatches", mismatch_contracts),
        ("mismatches_in_the_10_contracts_with_most", top10),
        ("control_one_planted_from_change_mismatches", "" if control_mismatches is None else control_mismatches),
        ("control_detected", "" if control_mismatches is None else ("yes" if control_mismatches == mismatches + 1 else "no"))])
    class_rows = [{"class": c, "mismatches": classes.get(c, 0),
                   "share_of_mismatches": exact_or_prefix(Fraction(classes.get(c, 0), mismatches), 6) if mismatches else "0"}
                  for c in CLASSES]
    write_csv(os.path.join(a.out, "erc721_from.csv"), [summary])
    write_csv(os.path.join(a.out, "erc721_from_days.csv"), per_day)
    write_csv(os.path.join(a.out, "erc721_from_mismatch_classes.csv"), class_rows)
    with open(os.path.join(a.out, "erc721_from_environment.json"), "w") as fh:
        json.dump({"started_utc": started, "finished_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   "source": BUCKET_URL + PREFIX, "duckdb": duckdb.__version__, "transfer_topic0": TRANSFER,
                   "git_commit": subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, stdout=subprocess.PIPE, text=True).stdout.strip(),
                   "files": [{"date": d["date"], "bytes": d["file_bytes"], "sha256": sha256(f)} for d, f in zip(per_day, files)]},
                  fh, indent=2)
    for k, v in summary.items():
        print("%-48s %s" % (k, v))
    for r in class_rows:
        print("  %-32s %s" % (r["class"], r["mismatches"]))


# ------------------------------------------------------------------- chain-check


def hexint(s):
    """An Etherscan hex quantity; Etherscan writes zero as "0x"."""
    return int(s, 16) if s not in ("0x", "") else 0


def explorer_get(api, key, params):
    """One GET to Etherscan's API V2 for Ethereum mainnet, as parsed JSON; (None, error) when the
    call fails. Rate-limit answers are retried after a back-off. The key goes only into the
    request URL: it is never printed, logged or written, and errors are recorded without it."""
    q = urllib.parse.urlencode(dict(params, chainid=CHAIN_ID, apikey=key))
    err = None
    for attempt in range(4):
        try:
            with get(api + "?" + q, 60) as r:
                res = json.loads(r.read())
            if res.get("status") == "0" and "rate limit" in str(res.get("result")).lower():
                err = str(res["result"])
            else:
                return res, None
        except Exception as exc:  # retry transient network errors, then give up on this call
            err = str(exc)
        time.sleep(2 * (attempt + 1))
    return None, err.replace(key, "<key>")[:120]


def explorer_logs(api, key, contract, token, from_block, to_block):
    """The token's Transfer logs in [from_block, to_block] (contract, topic0, tokenId as topic3), as
    (block number, log index) pairs; (None, error) when the call fails."""
    res, err = explorer_get(api, key, {"module": "logs", "action": "getLogs", "fromBlock": from_block, "toBlock": to_block,
                                       "address": contract, "topic0": TRANSFER, "topic3": token, "topic0_3_opr": "and",
                                       "page": 1, "offset": PAGE})
    if err is not None:
        return None, err
    result = res.get("result")
    if res.get("status") == "1" and isinstance(result, list):
        if len(result) >= PAGE:
            return None, "a full page of %d logs" % PAGE
        return sorted((hexint(l["blockNumber"]), hexint(l["logIndex"])) for l in result
                      if len(l["topics"]) >= 4 and l["topics"][3]), None
    if res.get("status") == "0" and result == []:  # "No records found"
        return [], None
    return None, str(result if isinstance(result, str) else res.get("message") or res).replace(key, "<key>")[:120]


def erc721_declared(api, key, contract):
    """eth_call supportsInterface(0x80ac58cd) at the latest block, through the API's proxy module."""
    res, err = explorer_get(api, key, {"module": "proxy", "action": "eth_call", "to": contract, "tag": "latest",
                                       "data": "0x01ffc9a7" + ERC721_INTERFACE[2:] + "0" * 56})
    call = None if err is not None or "error" in res else res.get("result")
    if not isinstance(call, str) or not call.startswith("0x") or call == "0x":
        return "error or no answer"
    return "declares ERC-721" if int(call, 16) == 1 else "does not declare ERC-721"


def chain_check(a):
    key = os.environ.get(KEY_ENV)
    if not key:
        raise SystemExit("set %s (exported) to an Etherscan API key" % KEY_ENV)
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    con = duckdb.connect()
    rows = con.execute("SELECT contract, token_topic, block_number, log_index, prev_block, class FROM read_parquet('%s') "
                       "ORDER BY block_number, log_index" % os.path.join(a.work, "%s.parquet" % a.population)).fetchall()
    sample = random.Random(a.seed).sample(rows, min(a.sample, len(rows)))
    tally, details = collections.Counter(), []
    for i, (contract, token, block, log_index, prev_block, cls) in enumerate(sample):
        ds = sorted(con.execute("SELECT block_number, log_index FROM read_parquet('%s') WHERE contract = ? AND token_topic = ? "
                                "AND block_number BETWEEN ? AND ?" % os.path.join(a.work, "transfers.parquet"),
                                [contract, token, prev_block, block]).fetchall())
        chain, err = explorer_logs(a.explorer, key, contract, token, prev_block, block)
        time.sleep(a.pause)
        if err is not None:
            fidelity = "explorer error (%s)" % err
        else:
            fidelity = ("dataset equals chain" if chain == ds else "chain has events the dataset lacks"
                        if set(ds) < set(chain) else "dataset and chain differ")
        erc165 = erc721_declared(a.explorer, key, contract)
        time.sleep(a.pause)
        tally[(cls, fidelity, erc165)] += 1
        details.append({"class": cls, "fidelity": fidelity, "erc165": erc165, "blocks_between": block - prev_block})
        print("%d/%d %s; %s" % (i + 1, len(sample), fidelity, erc165), flush=True)
    con.close()
    write_csv(os.path.join(a.work, "chain_check_%s_details.csv" % a.population), details)
    out = [{"population": a.population, "class": c, "dataset_against_chain": f, "erc165_erc721": e, "sampled": n}
           for (c, f, e), n in sorted(tally.items())]
    if not out:
        out = [{"population": a.population, "class": "", "dataset_against_chain": "", "erc165_erc721": "", "sampled": 0}]
    write_csv(os.path.join(a.out, "erc721_from_chain_check_%s.csv" % a.population), out)
    with open(os.path.join(a.out, "erc721_from_chain_check_%s_environment.json" % a.population), "w") as fh:
        json.dump({"started_utc": started, "checked_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                   "population": a.population, "sample": len(sample), "of": len(rows), "seed": a.seed,
                   "explorer": a.explorer, "chain_id": CHAIN_ID, "pause_seconds": a.pause,
                   "calls": "the logs API (contract, topic0, tokenId as topic3) from the predecessor's block to the "
                            "transfer's block, both included, compared with the dataset's events for the token in that "
                            "range; the proxy module's eth_call supportsInterface(0x80ac58cd) at latest",
                   "git_commit": subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, stdout=subprocess.PIPE,
                                                text=True).stdout.strip()}, fh, indent=2)
    for r in out:
        print(r)


# ----------------------------------------------------------------------- summary


def md(rows, headers):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def summary(a):
    s = next(csv.DictReader(open(os.path.join(a.out, "erc721_from.csv"))))
    classes = list(csv.DictReader(open(os.path.join(a.out, "erc721_from_mismatch_classes.csv"))))
    checks = []
    for pop in ("mismatches", "matches"):
        f = os.path.join(a.out, "erc721_from_chain_check_%s.csv" % pop)
        if os.path.exists(f):
            checks.append((list(csv.DictReader(open(f))),
                           json.load(open(os.path.join(a.out, "erc721_from_chain_check_%s_environment.json" % pop)))))
    fmt = lambda v: "{:,}".format(int(v))  # noqa: E731
    lines = [SECTION_HEADING, ""]
    w = lines.append
    w("Measured by `study/ethereum/erc721_from.py` on the AWS Public Blockchain Data (`s3://aws-public-blockchain/v1.0/eth/logs/`, "
      "public Ethereum mainnet data, not synthetic): %s daily files from %s (%s bytes, %s logs, blocks %s to %s). An ERC-721 "
      "Transfer is a log with topic0 = keccak256(\"Transfer(address,address,uint256)\") and four topics (from, to, tokenId). "
      "Within each token (contract and tokenId), transfers are ordered by block number and log index, and LAG(to) is compared "
      "with from for every transfer with a predecessor in the window; mints (from is the zero address) are excluded."
      % (s["files"], s["window_start"], fmt(s["file_bytes"]), fmt(s["logs"]), fmt(s["first_block"]), fmt(s["last_block"])))
    w("")
    w(md([["ERC-721 Transfer events", fmt(s["erc721_transfer_events"])],
          ["Repeated (block number, log index) among them", fmt(s["erc721_repeated_block_log_index"])],
          ["Transfers", fmt(s["transfers"])], ["Contracts", fmt(s["contracts"])], ["Tokens", fmt(s["tokens"])],
          ["Mints (from zero)", fmt(s["mints_from_zero"])], ["Burns (to zero)", fmt(s["burns_to_zero"])],
          ["Transfers with a predecessor in the window", fmt(s["transfers_with_predecessor"])],
          ["Compared (mints excluded)", fmt(s["compared_excluding_mints"])],
          ["from equals LAG(to)", fmt(s["from_equals_lag_to"])], ["Mismatches", fmt(s["mismatches"])],
          ["Exact match rate", s["match_rate"]],
          ["Contracts with a mismatch", fmt(s["contracts_with_mismatches"])],
          ["Mismatches in the 10 contracts with the most", fmt(s["mismatches_in_the_10_contracts_with_most"])],
          ["ERC-721 contracts that also emit three-topic Transfer events", fmt(s["contracts_also_emitting_erc20_style_transfer"])],
          ["Log index order against transaction index order, disagreements", fmt(s["log_index_order_against_transaction_index"])],
          ["Control: mismatches after changing one matched transfer's from", s["control_one_planted_from_change_mismatches"]],
          ["Control detected", s["control_detected"]]],
         ["Measure", "Value"]))
    w("")
    w(md([[c["class"], fmt(c["mismatches"]), c["share_of_mismatches"]] for c in classes],
         ["Mismatch class", "Mismatches", "Share of mismatches"]))
    w("")
    w("Classes, checked in this order: same_block_ordering, from is the `to` of another transfer of the token in the same "
      "block; non_standard_contract, the contract also emits three-topic (ERC-20 style) Transfer events in the window; "
      "previous_transfer_not_captured, from is never the `to` of a captured transfer of the token in the window; other, "
      "anything else.")
    w("")
    if checks:
        w("Dataset against chain: \"dataset equals chain\" when the API returns exactly the dataset's Transfer events of "
          "the token from the predecessor's block to the transfer's block; for a matched pair, that means the chain has no "
          "Transfer of the token between the two that the dataset lacks.")
        w("")
    for chain, cenv in checks:
        w("Chain check of %s: %d drawn at random (seed %d) of %s, each checked through Etherscan's API V2 (`%s`, chain id "
          "%d) from %s to %s, with a %s s pause between calls: %s." % (
              cenv["population"], cenv["sample"], cenv["seed"], fmt(cenv["of"]), cenv["explorer"], cenv["chain_id"],
              cenv["started_utc"], cenv["checked_utc"], cenv["pause_seconds"], cenv["calls"]))
        w("")
        if cenv["sample"]:
            w(md([[r["class"], r["dataset_against_chain"], r["erc165_erc721"], r["sampled"]] for r in chain],
                 ["Class", "Dataset against chain", "ERC-165 answer", "Sampled"]))
            w("")
    w("Files: `ethereum/erc721_from.csv`, `erc721_from_days.csv`, `erc721_from_mismatch_classes.csv`, "
      "`erc721_from_chain_check_*.csv` and their environment files. Aggregates only: no contract, token, wallet or "
      "transaction identifiers.")
    w("")
    replace_section(a.summary, SECTION_HEADING, "\n".join(lines))
    print("\n".join(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["download", "analyze", "chain-check", "summary"])
    ap.add_argument("--start", help="first day, YYYY-MM-DD")
    ap.add_argument("--days", type=int)
    ap.add_argument("--raw-dir")
    ap.add_argument("--work")
    ap.add_argument("--out", default="results/ethereum")
    ap.add_argument("--summary", default="results/SUMMARY.md")
    ap.add_argument("--sample", type=int, default=50)
    ap.add_argument("--seed", type=int, default=20261028)
    ap.add_argument("--explorer", default=EXPLORER, help="Etherscan API V2 endpoint (key from %s)" % KEY_ENV)
    ap.add_argument("--pause", type=float, default=1.0, help="seconds between API calls")
    ap.add_argument("--population", choices=["mismatches", "matches"], default="mismatches",
                    help="sample mismatches, or matched comparisons (a check that the chain has no transfer between them)")
    a = ap.parse_args()
    {"download": download, "analyze": analyze, "chain-check": chain_check, "summary": summary}[a.step](a)


if __name__ == "__main__":
    main()
