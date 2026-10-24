package com.example.audit.aws;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Reads the step files bench/aws/generate_steps.py writes: one file per event index
 * (step-001.bin holds every entity's first event, in entity order), each a sequence of
 * frames [4-byte big-endian length][Avro binary datum]. Reading steps 1..H in order gives
 * the events in (event_timestamp, customer_id) order, the order the local pipeline sends.
 */
public final class StepFiles {

    private StepFiles() {
    }

    public static Path step(Path dir, int step) {
        return dir.resolve(String.format("step-%03d.bin", step));
    }

    /** Sequential reader over the frames of one file. */
    public static final class Reader implements Closeable {
        private final DataInputStream in;

        public Reader(Path file) throws IOException {
            this.in = new DataInputStream(new BufferedInputStream(Files.newInputStream(file), 8 << 20));
        }

        /** The next datum, or null at the end of the file. */
        public byte[] next() throws IOException {
            int len;
            try {
                len = in.readInt();
            } catch (EOFException end) {
                return null;
            }
            byte[] datum = new byte[len];
            in.readFully(datum);
            return datum;
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    /** customer_id, the first field of every design's record: an Avro string (zig-zag varint length, UTF-8 bytes). */
    public static byte[] customerId(byte[] datum) {
        long raw = 0;
        int shift = 0;
        int pos = 0;
        while (true) {
            int b = datum[pos++] & 0xff;
            raw |= (long) (b & 0x7f) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        int len = (int) ((raw >>> 1) ^ -(raw & 1));
        byte[] key = new byte[len];
        System.arraycopy(datum, pos, key, 0, len);
        return key;
    }

    public static String customerIdString(byte[] datum) {
        return new String(customerId(datum), StandardCharsets.UTF_8);
    }
}
