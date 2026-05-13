package com.example.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

// Uses JSON to keep the example self-contained. In production, swap this for
// Avro deserialization backed by a schema registry; the schema to register is
// in src/main/avro/customer_profile_change_event.avsc.
public class CustomerProfileChangeDeserializer
        implements DeserializationSchema<CustomerProfileChangeEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public CustomerProfileChangeEvent deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, CustomerProfileChangeEvent.class);
    }

    @Override
    public boolean isEndOfStream(CustomerProfileChangeEvent event) {
        return false;
    }

    @Override
    public TypeInformation<CustomerProfileChangeEvent> getProducedType() {
        return TypeInformation.of(CustomerProfileChangeEvent.class);
    }
}
