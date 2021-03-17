/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.hive;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestMetadataCleanup {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "data", Types.StringType.get())
    );

    private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
            .bucket("data", 16)
            .build();

    private static final TableIdentifier NAME = TableIdentifier.of("default", "test");

    private static TestHiveMetastore metastore = null;
    private static HiveCatalog catalog = null;
    protected static HiveConf hiveConf = null;

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    private static Table table = null;

    @BeforeClass
    public static void startMetastoreAndCreateTable() {
        metastore = new TestHiveMetastore();
        metastore.start();
        hiveConf = metastore.hiveConf();
        catalog = (HiveCatalog)
                CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);
        table = catalog.createTable(NAME, SCHEMA, SPEC);

    }

    @AfterClass
    public static void stopMetastore() {
        metastore.stop();
        catalog.close();
        catalog = null;
        System.out.println("Hive Metastore CLOSED...");
    }

    private void writeTestDataFile() throws IOException {
        List<Record> records = Lists.newArrayList();

        // records all use IDs that are in bucket id_bucket=0
        GenericRecord record = GenericRecord.create(table.schema());
        records.add(record.copy("id", 29, "data", "a"));
        records.add(record.copy("id", 43, "data", "b"));
        records.add(record.copy("id", 61, "data", "c"));
        records.add(record.copy("id", 89, "data", "d"));
        records.add(record.copy("id", 100, "data", "e"));
        records.add(record.copy("id", 121, "data", "f"));
        records.add(record.copy("id", 122, "data", "g"));

        DataFile dataFile = writeDataFile(table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), records);

        table.newAppend()
                .appendFile(dataFile)
                .commit();
    }

    private DataFile writeDataFile(Table table, OutputFile out, StructLike partition, List<Record> rows)
            throws IOException {
        FileAppender<Record> writer = Parquet.write(out)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .schema(table.schema())
                .overwrite()
                .build();

        try (Closeable toClose = writer) {
            writer.addAll(rows);
        }

        return DataFiles.builder(table.spec())
                .withFormat(FileFormat.PARQUET)
                .withPath(out.location())
                .withPartition(partition)
                .withFileSizeInBytes(writer.length())
                .withSplitOffsets(writer.splitOffsets())
                .withMetrics(writer.metrics())
                .build();
    }

    @Test
    public void testExpiringSnapshotDeletion() throws IOException {
        writeTestDataFile();
        Assert.assertEquals(1, Iterables.size(table.snapshots()));
        long tsToExpire = System.currentTimeMillis();

        writeTestDataFile();
        writeTestDataFile();

        table.expireSnapshots()
                .expireOlderThan(tsToExpire)
                .commit();

        Assert.assertEquals("Should expire first snapshot keeping most recent 2",
                2,
                Iterables.size(table.snapshots())
        );
    }

}
