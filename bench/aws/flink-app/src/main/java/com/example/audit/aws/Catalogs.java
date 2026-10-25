package com.example.audit.aws;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;

import java.util.HashMap;
import java.util.Map;

/**
 * The Iceberg catalog for the run: the Glue Data Catalog with S3FileIO on AWS, or a Hadoop
 * catalog on a local path for tests. Tables are created the way the local pipeline benchmark
 * creates them (PipelineBenchmark.createTable): partitioned by day(event_timestamp), sort
 * order customer_id, event_timestamp, event_sequence, every other property at the Iceberg
 * default.
 */
public final class Catalogs {

    private Catalogs() {
    }

    /** catalog = glue | hadoop; warehouse = s3://bucket/prefix or file:///path. */
    public static CatalogLoader loader(String catalog, String warehouse) {
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", warehouse);
        if (catalog.equals("glue")) {
            props.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            return CatalogLoader.custom("glue", props, new org.apache.hadoop.conf.Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog");
        }
        if (catalog.equals("hadoop")) {
            return CatalogLoader.hadoop("hadoop", new org.apache.hadoop.conf.Configuration(), props);
        }
        throw new IllegalArgumentException("catalog must be glue or hadoop, not " + catalog);
    }

    /** Creates the table if it does not exist; returns true when it was created. */
    public static boolean createTable(CatalogLoader loader, String database, String name, Schema schema) {
        Catalog catalog = loader.loadCatalog();
        TableIdentifier id = TableIdentifier.of(database, name);
        if (catalog.tableExists(id)) return false;
        Table t = catalog.createTable(id, schema, PartitionSpec.builderFor(schema).day("event_timestamp").build());
        t.replaceSortOrder().asc("customer_id").asc("event_timestamp").asc("event_sequence").commit();
        return true;
    }

    public static TableLoader tableLoader(CatalogLoader loader, String database, String name) {
        return TableLoader.fromCatalog(loader, TableIdentifier.of(database, name));
    }

    public static Table load(CatalogLoader loader, String database, String name) {
        Table t = loader.loadCatalog().loadTable(TableIdentifier.of(database, name));
        t.refresh();
        return t;
    }

    public static boolean drop(CatalogLoader loader, String database, String name) {
        return loader.loadCatalog().dropTable(TableIdentifier.of(database, name), true);
    }
}
