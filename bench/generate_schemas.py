#!/usr/bin/env python3
"""Build the two schemas the benchmark compares, from one attribute list.

  customer_profile_previous_value.avsc : each attribute, plus a previous_<attr>
                                         companion for the mutable ones.
  customer_profile_audit_trail.avsc    : the attributes only.

So a mutable attribute costs two fields in the previous-value schema and one in the
audit-trail schema. Run this to regenerate the .avsc files; SchemaBenchmark.java
then measures the difference in lines, fields, and Avro bytes.

This is a representative 80-attribute profile. It shows the same effect at a smaller
scale so you can run it yourself.
"""
import json
import os

# (name, avro primitive type). The first MUTABLE_COUNT are "mutable" and so get a
# previous-value companion in the previous-value schema. A realistic customer
# profile: identity, contact, address, billing, preferences, flags, and ids.
ATTRIBUTES = [
    ("email", "string"), ("phone", "string"), ("secondary_phone", "string"),
    ("first_name", "string"), ("last_name", "string"), ("display_name", "string"),
    ("address_line1", "string"), ("address_line2", "string"), ("city", "string"),
    ("state", "string"), ("postal_code", "string"), ("country", "string"),
    ("billing_line1", "string"), ("billing_line2", "string"), ("billing_city", "string"),
    ("billing_state", "string"), ("billing_postal_code", "string"), ("billing_country", "string"),
    ("company", "string"), ("job_title", "string"), ("department", "string"),
    ("timezone", "string"), ("locale", "string"), ("preferred_language", "string"),
    ("currency", "string"), ("account_tier", "string"), ("account_status", "string"),
    ("loyalty_tier", "string"), ("loyalty_points", "long"), ("credit_limit", "double"),
    ("payment_method", "string"), ("card_last_four", "string"), ("card_brand", "string"),
    ("billing_cycle_day", "long"), ("tax_id", "string"), ("tax_exempt", "boolean"),
    ("marketing_opt_in", "boolean"), ("sms_opt_in", "boolean"), ("email_opt_in", "boolean"),
    ("push_opt_in", "boolean"), ("newsletter_frequency", "string"), ("contact_preference", "string"),
    ("referral_code", "string"), ("referred_by", "string"), ("segment", "string"),
    ("lifecycle_stage", "string"), ("lead_source", "string"), ("utm_campaign", "string"),
    ("preferred_store_id", "string"), ("default_shipping_method", "string"), ("gift_wrap_default", "boolean"),
    ("avatar_url", "string"), ("bio", "string"), ("website", "string"),
    ("twitter_handle", "string"), ("linkedin_url", "string"), ("date_of_birth", "string"),
    ("gender", "string"), ("household_size", "long"), ("annual_income_band", "string"),
    ("interests", "string"), ("favorite_category", "string"), ("preferred_brand", "string"),
    ("shoe_size", "string"), ("shirt_size", "string"), ("dietary_preference", "string"),
    # --- below here: non-mutable / system-managed (no previous companion) ---
    ("created_at", "long"), ("first_order_at", "long"), ("last_login_at", "long"),
    ("last_order_at", "long"), ("total_orders", "long"), ("lifetime_value", "double"),
    ("external_crm_id", "string"), ("external_billing_id", "string"), ("data_region", "string"),
    ("consent_version", "string"), ("source_system", "string"), ("schema_version", "long"),
    ("partition_hint", "string"),
]

MUTABLE_COUNT = 67  # first N attributes get a previous_<attr> companion

KEYS = [
    {"name": "customer_id", "type": "string"},
    {"name": "event_timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "event_sequence", "type": "long"},
]


def attr_field(name, avro_type):
    return {"name": name, "type": ["null", avro_type], "default": None}


def build(include_previous):
    fields = list(KEYS)
    for i, (name, avro_type) in enumerate(ATTRIBUTES):
        fields.append(attr_field(name, avro_type))
        if include_previous and i < MUTABLE_COUNT:
            fields.append(attr_field("previous_" + name, avro_type))
    return {
        "type": "record",
        "name": "CustomerProfile" + ("WithPrevious" if include_previous else "AuditTrail"),
        "namespace": "com.example.audit.bench",
        "fields": fields,
    }


def dump(schema, path):
    # One field per line so a line count reflects the field count honestly,
    # and so a diff between the two files reads as "the previous_* lines".
    lines = ['{', '  "type": "record",',
             '  "name": "%s",' % schema["name"],
             '  "namespace": "%s",' % schema["namespace"],
             '  "fields": [']
    for i, f in enumerate(schema["fields"]):
        comma = "," if i < len(schema["fields"]) - 1 else ""
        lines.append("    " + json.dumps(f) + comma)
    lines.append("  ]")
    lines.append("}")
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return len(lines) + 1


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    prev = build(include_previous=True)
    audit = build(include_previous=False)
    n_prev = dump(prev, os.path.join(here, "customer_profile_previous_value.avsc"))
    n_audit = dump(audit, os.path.join(here, "customer_profile_audit_trail.avsc"))
    print("previous-value schema: %d fields, %d lines" % (len(prev["fields"]), n_prev))
    print("audit-trail schema:    %d fields, %d lines" % (len(audit["fields"]), n_audit))
    print("removed: %d previous-value fields" % (len(prev["fields"]) - len(audit["fields"])))


if __name__ == "__main__":
    main()
