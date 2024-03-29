package com.cloudaware;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class DeduplicateColumnNameTest {

    @Test
    public void test2() throws IOException {
        test("/com/cloudaware/test_in.parquet", new String[]{"a", "b"});
    }

    @Test
    public void test5() throws IOException {
        test("/com/cloudaware/test_dup2.parquet", new String[]{"col_a", "col_b", "col_c", "col_d", "col_e"});
    }

    public void test(final String inputName, final String[] finalColumns) throws IOException {
        final URL url = DeduplicateColumnNameTest.class.getResource(inputName);
        final Configuration configuration = new Configuration();
        final HadoopInputFile hadoopInputFile = HadoopInputFile.fromPath(new Path(url.getPath()), configuration);
        final ParquetFileReader reader = ParquetFileReader.open(hadoopInputFile);
        final MessageType schema = reader.getFooter().getFileMetaData().getSchema();

        final ParquetToSparkSchemaConverter parquetToSparkSchemaConverter = new ParquetToSparkSchemaConverter(
                true,
                true,
                false,
                true,
                true
        );

        final StructType sparkSchema = parquetToSparkSchemaConverter.convert(schema);
        final int coreCount = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
        final SparkSession spark = SparkSession
                .builder()
                .master("local[" + coreCount + "]")
                .config("spark.sql.hive.convertMetastoreParquet", "false")
                .config("parquet.column.index.access", "true")
                .config("parquet.strict.typing", "false")
                .config("spark.sql.caseSensitive", "true")
                .appName("AWS Cost And Usage Report Filter")
                .config("parquet.writer.version", "v2")
                .config("spark.sql.parquet.binaryAsString", "true")
                .config("spark.sql.parquet.enableVectorizedReader", "false")
                .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
                .getOrCreate();
        //read parquet
        final Dataset<Row> rowDataset = spark.read()
                .schema(sparkSchema)
                .parquet(url.getFile());
        rowDataset.show();
        final File output = File.createTempFile("dedup_out", ".snappy.parquet");
        rowDataset.write().mode(SaveMode.Overwrite).parquet(output.getAbsolutePath());
        final Dataset<Row> newDataset = spark.read().parquet(output.getAbsolutePath());
        Assertions.assertEquals(3, newDataset.count());
        Assertions.assertEquals(finalColumns.length, newDataset.schema().fieldNames().length);
        Assertions.assertArrayEquals(finalColumns, newDataset.schema().fieldNames());
        spark.stop();
        output.delete();
    }
}
