"""Describe the schema and the prior_state block of captured page-change events.

Prints field paths and counts only, never values, so the output carries no titles,
user names or page content.

  python3 study/wikimedia/inspect_schema.py --in /some/dir/probe.jsonl
"""
import argparse
import collections
import json


def leaves(obj, prefix=""):
    """Yield (dotted path, value) for every non-object leaf."""
    for k, v in obj.items():
        path = prefix + k
        if isinstance(v, dict) and v:
            yield from leaves(v, path + ".")
        else:
            yield path, v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    args = ap.parse_args()

    schemas = collections.Counter()
    streams = collections.Counter()
    kinds = collections.Counter()
    changelog = collections.Counter()
    top_fields = collections.Counter()
    with_prior = collections.Counter()
    prior_paths = collections.defaultdict(collections.Counter)
    # Per kind and prior_state path: how often the prior value equals the current value.
    prior_equal = collections.defaultdict(collections.Counter)
    prior_has_current = collections.defaultdict(collections.Counter)
    # Per kind and sub-object: current leaves seen, of which present in prior_state.
    coverage = collections.defaultdict(lambda: [0, 0])
    n = 0
    with open(args.inp, encoding="utf-8") as f:
        for line in f:
            ev = json.loads(json.loads(line)["d"])
            n += 1
            schemas[ev.get("$schema")] += 1
            streams[ev.get("meta", {}).get("stream")] += 1
            kind = ev.get("page_change_kind")
            kinds[kind] += 1
            changelog[(kind, ev.get("changelog_kind"))] += 1
            for k in ev:
                top_fields[k] += 1
            prior = ev.get("prior_state")
            if not prior:
                continue
            with_prior[kind] += 1
            current = dict(leaves({k: ev[k] for k in prior if isinstance(ev.get(k), dict)}))
            prior_flat = dict(leaves(prior))
            for path, value in prior_flat.items():
                prior_paths[kind][path] += 1
                if path in current:
                    prior_has_current[kind][path] += 1
                    if current[path] == value:
                        prior_equal[kind][path] += 1
            for sub in prior:
                if isinstance(ev.get(sub), dict):
                    cur_sub = dict(leaves(ev[sub]))
                    pri_sub = dict(leaves(prior[sub])) if isinstance(prior[sub], dict) else {}
                    coverage[(kind, sub)][0] += len(cur_sub)
                    coverage[(kind, sub)][1] += sum(1 for p in cur_sub if p in pri_sub)

    print("events", n)
    print("$schema", dict(schemas))
    print("meta.stream", dict(streams))
    print("top-level fields", dict(top_fields))
    print("page_change_kind", dict(kinds))
    print("(page_change_kind, changelog_kind)", {"{}/{}".format(*k): v for k, v in changelog.items()})
    for kind in sorted(kinds, key=str):
        print()
        print("kind", kind, "events", kinds[kind], "with prior_state", with_prior[kind])
        for path, c in sorted(prior_paths[kind].items()):
            print("  prior_state.{:<45} present {:>6}  current field present {:>6}  equal to current {:>6}".format(
                path, c, prior_has_current[kind][path], prior_equal[kind][path]))
        for (k, sub), (cur, pri) in sorted(coverage.items()):
            if k == kind:
                print("  {}: current leaf fields {}, of which also in prior_state {}".format(sub, cur, pri))


if __name__ == "__main__":
    main()
