package com.example.audit.bench;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the field-count claim: the previous-value schema carries exactly 67 redundant
 * fields that the audit-trail schema drops.
 */
class SchemaBenchmarkTest {

    private Schema parse(String name) throws Exception {
        return new Schema.Parser().parse(Path.of("bench", name).toFile());
    }

    @Test
    void auditTrailDrops67PreviousValueFields() throws Exception {
        Schema prev = parse("customer_profile_previous_value.avsc");
        Schema audit = parse("customer_profile_audit_trail.avsc");
        assertEquals(67, prev.getFields().size() - audit.getFields().size(),
            "67 redundant previous-value fields");
        assertTrue(audit.getFields().stream().noneMatch(f -> f.name().startsWith("previous_")),
            "audit-trail schema must contain no previous_* fields");
    }
}
