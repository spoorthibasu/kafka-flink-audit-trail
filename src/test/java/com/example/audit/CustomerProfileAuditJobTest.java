package com.example.audit;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CustomerProfileAuditJobTest {

    @Test
    void toRowData_allFieldsSet() {
        CustomerProfileChangeEvent event = new CustomerProfileChangeEvent(
            "98765", 1_000L, 1L, "alice@gmail.com", "555-1001", "123 Main St"
        );

        RowData row = CustomerProfileAuditJob.toRowData(event);

        assertEquals(StringData.fromString("98765"), row.getString(0));
        assertEquals(1_000_000L, row.getLong(1)); // ms -> us
        assertEquals(1L, row.getLong(2));
        assertEquals(StringData.fromString("alice@gmail.com"), row.getString(3));
        assertEquals(StringData.fromString("555-1001"), row.getString(4));
        assertEquals(StringData.fromString("123 Main St"), row.getString(5));
    }

    @Test
    void toRowData_sparseEvent_nullFieldsPreserved() {
        // Only email changed on this event; phone and address are null
        CustomerProfileChangeEvent event = new CustomerProfileChangeEvent(
            "98765", 2_000L, 2L, "alice@yahoo.com", null, null
        );

        RowData row = CustomerProfileAuditJob.toRowData(event);

        assertEquals(StringData.fromString("alice@yahoo.com"), row.getString(3));
        assertTrue(row.isNullAt(4), "phone should be null for sparse event");
        assertTrue(row.isNullAt(5), "address should be null for sparse event");
    }

    @Test
    void toRowData_timestampConvertedToMicroseconds() {
        long millis = 1_700_000_000_000L;
        CustomerProfileChangeEvent event = new CustomerProfileChangeEvent(
            "98765", millis, 1L, null, null, null
        );

        RowData row = CustomerProfileAuditJob.toRowData(event);

        assertEquals(millis * 1_000L, row.getLong(1));
    }

    @Test
    void deserializer_validJson() throws Exception {
        String json = "{\"customerId\":\"98765\",\"eventTimestamp\":1000,\"eventSequence\":1,"
            + "\"email\":\"alice@gmail.com\",\"phone\":\"555-1001\",\"address\":null}";

        CustomerProfileChangeDeserializer deserializer = new CustomerProfileChangeDeserializer();
        CustomerProfileChangeEvent event = deserializer.deserialize(json.getBytes());

        assertEquals("98765", event.getCustomerId());
        assertEquals(1000L, event.getEventTimestamp());
        assertEquals(1L, event.getEventSequence());
        assertEquals("alice@gmail.com", event.getEmail());
        assertEquals("555-1001", event.getPhone());
        assertNull(event.getAddress());
    }

    @Test
    void deserializer_sparseJson_missingFieldsAreNull() throws Exception {
        // Only email changed; phone and address omitted from the message
        String json = "{\"customerId\":\"98765\",\"eventTimestamp\":2000,\"eventSequence\":2,"
            + "\"email\":\"alice@yahoo.com\"}";

        CustomerProfileChangeDeserializer deserializer = new CustomerProfileChangeDeserializer();
        CustomerProfileChangeEvent event = deserializer.deserialize(json.getBytes());

        assertEquals("alice@yahoo.com", event.getEmail());
        assertNull(event.getPhone());
        assertNull(event.getAddress());
    }
}
