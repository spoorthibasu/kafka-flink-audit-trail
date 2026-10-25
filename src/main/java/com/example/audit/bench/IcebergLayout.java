package com.example.audit.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.iceberg.SortField;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One audit-trail table for bench/iceberg_layout.py, kept on disk.
 *
 * Writes the events with the Iceberg FlinkSink exactly as PipelineBenchmark.icebergMode does
 * (same table creation, same number of bounded append jobs, same slices, same parallelism),
 * then runs the same snapshot expiration (retainLast 1) and the same compaction
 * (Actions.rewriteDataFiles with Iceberg's defaults). It does not expire snapshots after the
 * compaction, so the snapshot as written keeps its data files and can be read back or
 * rewritten later. Writes the two snapshot IDs, the table location, the declared sort order,
 * the table properties and Iceberg's Parquet write defaults to a JSON file.
 *
 *   IcebergLayout --events events_full_B.avro --warehouse /tmp/wh --commits 20 --parallelism 3 --out snaps.json
 */
public final class IcebergLayout {

    private IcebergLayout() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> o = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            o.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        for (String k : List.of("events", "warehouse", "commits", "parallelism", "out")) {
            if (!o.containsKey(k)) throw new IllegalArgumentException("missing --" + k);
        }
        int commits = Integer.parseInt(o.get("commits"));
        int parallelism = Integer.parseInt(o.get("parallelism"));
        PipelineBenchmark.Events ev = PipelineBenchmark.Events.read(Path.of(o.get("events")));

        // The appends of PipelineBenchmark.icebergMode, one run.
        long maxSeq = ev.sequences.stream().mapToLong(Long::longValue).max().orElse(0);
        if (maxSeq % commits != 0) throw new IllegalArgumentException("H=" + maxSeq + " is not divisible by " + commits + " commits");
        long perCommit = maxSeq / commits;
        TableLoader loader = PipelineBenchmark.createTable("file://" + o.get("warehouse"), "events", ev.iceberg);
        for (int c = 0; c < commits; c++) {
            long lo = c * perCommit + 1, hi = (c + 1) * perCommit;
            List<RowData> slice = new ArrayList<>();
            for (int i = 0; i < ev.rows.size(); i++) {
                long s = ev.sequences.get(i);
                if (s >= lo && s <= hi) slice.add(ev.rows.get(i));
            }
            PipelineBenchmark.rowsForAppend = slice;
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            env.setParallelism(parallelism);
            DataStream<RowData> rows = env.fromSequence(0, slice.size() - 1)
                .map(new PipelineBenchmark.FromHolder()).returns(InternalTypeInfo.of(ev.rowType));
            FlinkSink.forRowData(rows).tableLoader(loader).uidPrefix("iceberg-audit-trail").overwrite(false).append();
            env.execute("append " + (c + 1) + " of " + commits);
        }
        PipelineBenchmark.rowsForAppend = Collections.emptyList();
        Table table = PipelineBenchmark.load(loader);
        long written = table.currentSnapshot().snapshotId();

        // The expiration and compaction of PipelineBenchmark.icebergMode.
        PipelineBenchmark.expire(table);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        Actions.forTable(env, table).rewriteDataFiles().execute();
        table.refresh();
        long binpack = table.currentSnapshot().snapshotId();

        ObjectMapper json = new ObjectMapper();
        ObjectNode out = json.createObjectNode();
        out.put("table_location", table.location());
        out.put("snapshot_as_written", written);
        out.put("snapshot_after_binpack", binpack);
        out.put("events", ev.rows.size());
        List<String> sortFields = new ArrayList<>();
        for (SortField f : table.sortOrder().fields()) {
            sortFields.add(table.schema().findColumnName(f.sourceId()) + " " + f.direction() + " " + f.nullOrder());
        }
        out.put("declared_sort_order", String.join(", ", sortFields));
        ObjectNode props = out.putObject("table_properties");
        table.properties().forEach(props::put);
        ObjectNode defaults = out.putObject("iceberg_parquet_defaults");
        // A table without the codec property gets PARQUET_COMPRESSION_DEFAULT; new tables
        // have had PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0 written into their properties.
        defaults.put(TableProperties.PARQUET_COMPRESSION + " when unset", TableProperties.PARQUET_COMPRESSION_DEFAULT);
        defaults.put(TableProperties.PARQUET_COMPRESSION + " on new tables", TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0);
        defaults.put(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT);
        defaults.put(TableProperties.PARQUET_PAGE_SIZE_BYTES, TableProperties.PARQUET_PAGE_SIZE_BYTES_DEFAULT);
        defaults.put(TableProperties.PARQUET_DICT_SIZE_BYTES, TableProperties.PARQUET_DICT_SIZE_BYTES_DEFAULT);
        defaults.put(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);
        json.writerWithDefaultPrettyPrinter().writeValue(new File(o.get("out")), out);
        System.out.println("iceberg-layout written " + written + " binpack " + binpack + " at " + table.location());
    }
}
