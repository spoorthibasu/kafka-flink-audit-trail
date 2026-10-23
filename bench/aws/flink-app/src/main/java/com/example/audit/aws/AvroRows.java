package com.example.audit.aws;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Avro datum to Iceberg row conversion. The same mapping as
 * com.example.audit.bench.PipelineBenchmark in the root project (key columns required,
 * attributes optional, event_timestamp as a timestamp without zone), so the AWS tables have
 * the schema the local pipeline measurements used.
 */
public final class AvroRows {

    private AvroRows() {
    }

    enum Kind { STRING, LONG, DOUBLE, BOOLEAN, TIMESTAMP }

    /** The benchmark schema for design A, B or C, from results/schema_{A,B,C}.avsc. */
    public static Schema designSchema(String design) throws IOException {
        String path = "/schemas/schema_" + design + ".avsc";
        try (InputStream in = AvroRows.class.getResourceAsStream(path)) {
            if (in == null) throw new IllegalArgumentException("no schema resource " + path);
            return new Schema.Parser().parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    static Kind[] kinds(Schema avro) {
        Kind[] k = new Kind[avro.getFields().size()];
        for (int i = 0; i < k.length; i++) {
            Schema.Field f = avro.getFields().get(i);
            if (f.name().equals("event_timestamp")) {
                k[i] = Kind.TIMESTAMP;
                continue;
            }
            switch (base(f.schema()).getType()) {
                case STRING: k[i] = Kind.STRING; break;
                case LONG: k[i] = Kind.LONG; break;
                case DOUBLE: k[i] = Kind.DOUBLE; break;
                case BOOLEAN: k[i] = Kind.BOOLEAN; break;
                default: throw new IllegalArgumentException("unsupported type for " + f.name());
            }
        }
        return k;
    }

    static Schema base(Schema s) {
        if (s.getType() != Schema.Type.UNION) return s;
        for (Schema t : s.getTypes()) {
            if (t.getType() != Schema.Type.NULL) return t;
        }
        throw new IllegalArgumentException("null-only union");
    }

    static RowData toRow(GenericRecord r, Kind[] kinds) {
        GenericRowData row = new GenericRowData(kinds.length);
        for (int i = 0; i < kinds.length; i++) {
            Object v = r.get(i);
            if (v == null) continue;
            switch (kinds[i]) {
                case STRING: row.setField(i, StringData.fromString(v.toString())); break;
                case TIMESTAMP: row.setField(i, TimestampData.fromEpochMillis((Long) v)); break;
                default: row.setField(i, v);
            }
        }
        return row;
    }

    public static org.apache.iceberg.Schema icebergSchema(Schema avro) {
        List<Types.NestedField> fields = new ArrayList<>();
        int id = 1;
        for (Schema.Field f : avro.getFields()) {
            switch (f.name()) {
                case "customer_id": fields.add(Types.NestedField.required(id++, f.name(), Types.StringType.get())); break;
                case "event_timestamp": fields.add(Types.NestedField.required(id++, f.name(), Types.TimestampType.withoutZone())); break;
                case "event_sequence": fields.add(Types.NestedField.required(id++, f.name(), Types.LongType.get())); break;
                default:
                    Type t;
                    switch (base(f.schema()).getType()) {
                        case STRING: t = Types.StringType.get(); break;
                        case LONG: t = Types.LongType.get(); break;
                        case DOUBLE: t = Types.DoubleType.get(); break;
                        case BOOLEAN: t = Types.BooleanType.get(); break;
                        default: throw new IllegalArgumentException(f.name());
                    }
                    fields.add(Types.NestedField.optional(id++, f.name(), t));
            }
        }
        return new org.apache.iceberg.Schema(fields);
    }

    public static RowType rowType(Schema avro) {
        return FlinkSchemaUtil.convert(icebergSchema(avro));
    }

    /** Decodes one datum with a reused decoder; used by the runner tools for the C table load. */
    static final class Decoder {
        private final GenericDatumReader<GenericRecord> reader;
        private final Kind[] kinds;
        private BinaryDecoder decoder;

        Decoder(Schema schema) {
            this.reader = new GenericDatumReader<>(schema);
            this.kinds = kinds(schema);
        }

        RowData row(byte[] datum) throws IOException {
            decoder = DecoderFactory.get().binaryDecoder(datum, decoder);
            return toRow(reader.read(null, decoder), kinds);
        }
    }

    /** Avro datum to RowData inside the Flink job, as the Kafka value deserializer. */
    public static final class RowDataDeserializer implements DeserializationSchema<RowData> {
        private final String schemaJson;
        private final RowType rowType;
        private transient Decoder decoder;

        public RowDataDeserializer(Schema schema, RowType rowType) {
            this.schemaJson = schema.toString();
            this.rowType = rowType;
        }

        @Override
        public RowData deserialize(byte[] message) throws IOException {
            if (decoder == null) {
                decoder = new Decoder(new Schema.Parser().parse(schemaJson));
            }
            return decoder.row(message);
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return InternalTypeInfo.of(rowType);
        }
    }
}
