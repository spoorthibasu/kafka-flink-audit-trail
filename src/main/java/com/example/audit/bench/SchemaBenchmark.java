package com.example.audit.bench;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema size comparison: previous-value schema vs audit-trail schema.
 *
 * Reads the two schemas that bench/generate_schemas.py builds from one attribute list:
 *   customer_profile_previous_value.avsc  attributes plus a previous_* companion each
 *   customer_profile_audit_trail.avsc     attributes only
 *
 * Prints the schema lines, field counts, and the Avro binary size of a representative
 * event for each, with the reduction.
 *
 * This is a representative 80-attribute profile, kept small so you can run it.
 */
public class SchemaBenchmark {

    private static final Path BENCH_DIR = Path.of("bench");
    private static final Path PREV_AVSC = BENCH_DIR.resolve("customer_profile_previous_value.avsc");
    private static final Path AUDIT_AVSC = BENCH_DIR.resolve("customer_profile_audit_trail.avsc");

    /** Ordered pool of attributes an event may change, with representative values.
     *  All are mutable (have a previous_* companion), so they exercise the overhead. */
    private static final Map<String, String> POOL = new LinkedHashMap<>();
    static {
        POOL.put("email", "alice.johnson@example.com");
        POOL.put("phone", "+1-555-867-5309");
        POOL.put("secondary_phone", "+1-555-201-7788");
        POOL.put("first_name", "Alice");
        POOL.put("last_name", "Johnson");
        POOL.put("display_name", "Alice Johnson");
        POOL.put("address_line1", "1742 Meridian Avenue");
        POOL.put("address_line2", "Suite 400");
        POOL.put("city", "San Francisco");
        POOL.put("state", "CA");
        POOL.put("postal_code", "94103");
        POOL.put("country", "US");
        POOL.put("company", "Northwind Traders");
        POOL.put("job_title", "Procurement Lead");
        POOL.put("department", "Operations");
        POOL.put("timezone", "America/Los_Angeles");
        POOL.put("locale", "en-US");
        POOL.put("preferred_language", "English");
        POOL.put("currency", "USD");
        POOL.put("account_tier", "Gold");
    }

    /** System/derived context every event carries. These attributes have NO previous_*
     *  companion (they are not mutable profile fields), so they enlarge both schemas
     *  equally, which is exactly why a real event's reduction is smaller than a
     *  changed-fields-only event's. Present on every event regardless of what changed. */
    private static final Map<String, Object> ALWAYS = new LinkedHashMap<>();
    static {
        ALWAYS.put("created_at", 1_650_000_000_000L);
        ALWAYS.put("first_order_at", 1_655_000_000_000L);
        ALWAYS.put("last_login_at", 1_699_000_000_000L);
        ALWAYS.put("last_order_at", 1_699_500_000_000L);
        ALWAYS.put("total_orders", 42L);
        ALWAYS.put("lifetime_value", 3791.55);
        ALWAYS.put("external_crm_id", "CRM-000123456");
        ALWAYS.put("external_billing_id", "BILL-77654321");
        ALWAYS.put("data_region", "us-west-2");
        ALWAYS.put("consent_version", "v3.2");
        ALWAYS.put("source_system", "profile-service");
        ALWAYS.put("schema_version", 5L);
    }

    /** How many mutable fields a representative event changes. Byte size is population-
     *  dependent, so we show a range. */
    private static final int[] CHANGE_COUNTS = {3, 8, 16};

    public static void main(String[] args) throws Exception {
        if (!Files.exists(PREV_AVSC) || !Files.exists(AUDIT_AVSC)) {
            System.err.println("Missing bench schemas. Run: python3 bench/generate_schemas.py");
            System.exit(1);
        }

        Schema prev = new Schema.Parser().parse(PREV_AVSC.toFile());
        Schema audit = new Schema.Parser().parse(AUDIT_AVSC.toFile());

        int prevLines = Files.readAllLines(PREV_AVSC).size();
        int auditLines = Files.readAllLines(AUDIT_AVSC).size();
        int prevFields = prev.getFields().size();
        int auditFields = audit.getFields().size();

        System.out.println();
        System.out.println("Previous-value schema vs audit-trail schema");
        System.out.println("(representative 80-attribute customer profile)");
        System.out.println();
        System.out.printf("  %-30s %14s %14s %12s%n", "", "previous-value", "audit-trail", "reduction");
        row("schema definition lines", prevLines, auditLines);
        row("schema fields", prevFields, auditFields);
        for (int k : CHANGE_COUNTS) {
            row(String.format("Avro binary bytes (%2d changed)", k),
                encodedSize(prev, true, k), encodedSize(audit, false, k));
        }
        System.out.println();
        System.out.printf("  previous-value fields removed: %d%n", prevFields - auditFields);
        System.out.println();
        System.out.println("  Byte size depends on how many fields an event changes.");
        System.out.println();
    }

    private static void row(String label, int prev, int audit) {
        double pct = prev == 0 ? 0 : 100.0 * (prev - audit) / prev;
        System.out.printf("  %-30s %14d %14d %11.0f%%%n", label, prev, audit, pct);
    }

    /** Avro binary size of an event that changes the first {@code k} pooled attributes. */
    private static int encodedSize(Schema schema, boolean withPrevious, int k) throws Exception {
        List<String> changed = changedAttributes(k);
        GenericRecord record = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            record.put(field.name(), valueFor(field.name(), withPrevious, changed));
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        writer.write(record, encoder);
        encoder.flush();
        return out.size();
    }

    private static Object valueFor(String name, boolean withPrevious, List<String> changed) {
        switch (name) {
            case "customer_id":     return "98765";
            case "event_timestamp": return 1_700_000_000_000L;
            case "event_sequence":  return 7L;
            default:
                if (ALWAYS.containsKey(name)) {
                    return ALWAYS.get(name);
                }
                if (changed.contains(name)) {
                    return POOL.get(name);
                }
                if (withPrevious && name.startsWith("previous_")) {
                    String base = name.substring("previous_".length());
                    if (changed.contains(base)) {
                        return "prior-" + POOL.get(base); // the value before the change
                    }
                }
                return null; // unchanged attribute: null in this event
        }
    }

    /** The first {@code k} attributes from the pool. Exposed for the test. */
    static List<String> changedAttributes(int k) {
        return new ArrayList<>(POOL.keySet()).subList(0, Math.min(k, POOL.size()));
    }
}
