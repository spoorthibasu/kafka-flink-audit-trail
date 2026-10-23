package com.example.audit.aws;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AvroRowsTest {

    static byte[] encode(Schema s, GenericRecord r) throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        BinaryEncoder enc = EncoderFactory.get().binaryEncoder(buf, null);
        new GenericDatumWriter<GenericRecord>(s).write(r, enc);
        enc.flush();
        return buf.toByteArray();
    }

    static GenericRecord record(Schema s) {
        GenericRecord r = new GenericData.Record(s);
        r.put("customer_id", "C0000042");
        r.put("event_timestamp", 1_700_000_060_042L);
        r.put("event_sequence", 2L);
        r.put("email", "abcdefghij");
        r.put("loyalty_points", 1234L);
        r.put("credit_limit", 99.5);
        r.put("tax_exempt", true);
        return r;
    }

    @Test
    void schemasAreTheBenchmarkSchemas() throws Exception {
        assertEquals(137, AvroRows.designSchema("A").getFields().size());
        assertEquals(70, AvroRows.designSchema("B").getFields().size());
        assertEquals(70, AvroRows.designSchema("C").getFields().size());
        assertEquals("previous_email", AvroRows.designSchema("A").getFields().get(4).name());
    }

    @Test
    void deserializerKeepsValuesAndNulls() throws Exception {
        Schema s = AvroRows.designSchema("A");
        GenericRecord r = record(s);
        r.put("previous_email", null);
        AvroRows.RowDataDeserializer d = new AvroRows.RowDataDeserializer(s, AvroRows.rowType(s));
        RowData row = d.deserialize(encode(s, r));
        assertEquals("C0000042", row.getString(0).toString());
        assertEquals(1_700_000_060_042L, row.getTimestamp(1, 3).getMillisecond());
        assertEquals(2L, row.getLong(2));
        assertEquals("abcdefghij", row.getString(3).toString());
        assertTrue(row.isNullAt(4));
        int lp = s.getField("loyalty_points").pos();
        assertEquals(1234L, row.getLong(lp));
        assertEquals(99.5, row.getDouble(s.getField("credit_limit").pos()));
        assertTrue(row.getBoolean(s.getField("tax_exempt").pos()));
        assertEquals(AvroRows.icebergSchema(s).columns().size(), s.getFields().size());
    }

    @Test
    void stepFramesAndKeys() throws Exception {
        Schema s = AvroRows.designSchema("B");
        byte[] datum = encode(s, record(s));
        assertArrayEquals("C0000042".getBytes(), StepFiles.customerId(datum));
        Path dir = Files.createTempDirectory("steps");
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(StepFiles.step(dir, 1)))) {
            out.writeInt(datum.length);
            out.write(datum);
            out.writeInt(datum.length);
            out.write(datum);
        }
        try (StepFiles.Reader r = new StepFiles.Reader(StepFiles.step(dir, 1))) {
            assertArrayEquals(datum, r.next());
            assertArrayEquals(datum, r.next());
            assertNull(r.next());
        }
    }
}
