"""Derive an Avro schema from Wikimedia's JSON schema for mediawiki/page/change, and
DuckDB types and field paths from that Avro schema.

  python3 study/wikimedia/schema.py \\
      --url https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/page/change/1.12.0 \\
      --out study/wikimedia/schema/page_change_1.12.0.avsc

The published schema is YAML (JSON Schema draft 7) and needs PyYAML. Rules, applied
recursively from the root object:

  object with properties                 -> record, fields in the schema's order
  object with only additionalProperties  -> map of the value type
  array                                  -> array of the item type
  string (also date-time, uri, enum)     -> string
  integer                                -> long
  number                                 -> double
  boolean                                -> boolean

A property not in its object's "required" list becomes the union ["null", T] with
default null. Avro names allow [A-Za-z0-9_] only, so "$schema" becomes "_schema"; every
field keeps its JSON key in a "json_name" attribute. Descriptions, examples, bounds and
enum lists are dropped. Records are named by their path from the root.
"""
import argparse
import json
import re
import urllib.request

AVRO = {"string": "string", "integer": "long", "number": "double", "boolean": "boolean"}
DUCK = {"string": "VARCHAR", "long": "BIGINT", "double": "DOUBLE", "boolean": "BOOLEAN"}


def avro_name(key):
    name = re.sub(r"[^A-Za-z0-9_]", "_", key)
    return name if re.match(r"[A-Za-z_]", name) else "_" + name


def to_avro(node, path):
    t = node.get("type")
    if t == "object":
        props = node.get("properties")
        if props:
            required = set(node.get("required", []))
            fields = []
            for key, sub in props.items():
                ftype = to_avro(sub, path + [avro_name(key)])
                field = {"name": avro_name(key), "json_name": key}
                if key in required:
                    field["type"] = ftype
                else:
                    field["type"] = ["null", ftype]
                    field["default"] = None
                fields.append(field)
            return {"type": "record", "name": "__".join(path), "fields": fields}
        if isinstance(node.get("additionalProperties"), dict):
            return {"type": "map", "values": to_avro(node["additionalProperties"], path + ["value"])}
        raise ValueError("object without properties at %s" % ".".join(path))
    if t == "array":
        return {"type": "array", "items": to_avro(node["items"], path + ["item"])}
    if t in AVRO:
        return AVRO[t]
    raise ValueError("unsupported type %r at %s" % (t, ".".join(path)))


def load_avro(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def non_null(t):
    if isinstance(t, list):
        rest = [x for x in t if x != "null"]
        return rest[0]
    return t


def duck_type(t, extra=None):
    """DuckDB type for an Avro type, with JSON keys as struct field names.
    extra maps a record name to additional (json_name, duckdb type) fields."""
    t = non_null(t)
    if isinstance(t, str):
        return DUCK[t]
    if t["type"] == "record":
        parts = ['"%s" %s' % (f["json_name"], duck_type(f["type"], extra)) for f in t["fields"]]
        parts += ['"%s" %s' % kv for kv in (extra or {}).get(t["name"], [])]
        return "STRUCT(%s)" % ", ".join(parts)
    if t["type"] == "map":
        return "MAP(VARCHAR, %s)" % duck_type(t["values"], extra)
    if t["type"] == "array":
        return "%s[]" % duck_type(t["items"], extra)
    raise ValueError(t)


def field(record, json_name):
    for f in record["fields"]:
        if f["json_name"] == json_name:
            return f
    raise KeyError(json_name)


def leaves(t, path=()):
    """(json path tuple, avro type) for every non-record field; maps and arrays are leaves."""
    t = non_null(t)
    if isinstance(t, dict) and t["type"] == "record":
        for f in t["fields"]:
            yield from leaves(f["type"], path + (f["json_name"],))
    else:
        yield path, t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    import yaml  # only needed to read the published schema
    req = urllib.request.Request(args.url, headers={"User-Agent": "audit-trail-bench-wikimedia-study/1.0 "
                                                    "(https://github.com/spoorthibasu/kafka-flink-audit-trail)"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        js = yaml.safe_load(resp.read())
    avro = to_avro(js, ["page_change"])
    avro["doc"] = "Derived by study/wikimedia/schema.py from %s (%s)" % (args.url, js.get("$id"))
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(avro, f, indent=2)
        f.write("\n")
    print("wrote %s: %d leaf fields" % (args.out, sum(1 for _ in leaves(avro))))


if __name__ == "__main__":
    main()
