package com.cloudaware;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.node.TextNode;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class CostAndUsageReportParquetFilter {
    private static final Pattern PERIOD_PATTERN = Pattern.compile("([12]\\d{3}(0[1-9]|1[0-2])(0[1-9]|[12]\\d|3[01]))-([12]\\d{3}(0[1-9]|1[0-2])(0[1-9]|[12]\\d|3[01]))");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int HTTP_NOT_FOUND = 404;
    private final AmazonS3Client amazonS3Client = new AmazonS3Client();

    {
        OBJECT_MAPPER.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    }

    public static void main(final String... args) throws Exception {
        final CostAndUsageReportParquetFilter app = new CostAndUsageReportParquetFilter();
        if (args.length != 5) {
            System.out.println("usage: java -jar cur-filter-1.0-SNAPSHOT.jar \"reportName\" \"reportPrefix\" \"inputBucket\" \"outputBucket\" \"comma separated linkedAccountId\"");
        } else {
            System.out.println("reportName:" + args[0]);
            System.out.println("reportPrefix:" + args[1]);
            System.out.println("inputBucket:" + args[2]);
            System.out.println("outputBucket:" + args[3]);
            System.out.println("linkedAccountId:" + args[4]);
            app.run(args[0], args[1], args[2], args[3], args[4]);
        }

        System.exit(0);
    }

    public void run(final String reportName, final String reportPrefix, final String inputBucket, final String outputBucket, final String linkedAccountIdsString) throws Exception {

        //get periods
        final ObjectListing objectListing = amazonS3Client
                .listObjects(
                        new ListObjectsRequest(
                                inputBucket,
                                reportPrefix,
                                null,
                                "/",
                                1000
                        )
                );
        List<String> periods = objectListing.getCommonPrefixes().stream().map(p -> p.substring(reportPrefix.length())).filter(p -> PERIOD_PATTERN.matcher(p).find()).collect(Collectors.toList());
        System.out.println("Founded periods: ");
        System.out.println(Joiner.on("\n").join(periods));
        //process periods
        for (final String period : periods) {
            //read manifest

            final JsonNode inputManifest = OBJECT_MAPPER.readTree(amazonS3Client.getObject(inputBucket, reportPrefix + period + reportName + "-Manifest.json").getObjectContent());
            JsonNode outputManifest = null;
            S3Object outputManifestObject = null;
            try {
                outputManifestObject = amazonS3Client.getObject(outputBucket, reportPrefix + period + reportName + "-Manifest.json");
            } catch (final AmazonS3Exception amazonS3Exception) {
                if (amazonS3Exception.getStatusCode() != HTTP_NOT_FOUND) {
                    throw amazonS3Exception;
                }
            }
            if (outputManifestObject != null) {
                outputManifest = OBJECT_MAPPER.readTree(outputManifestObject.getObjectContent());
            }
            if (outputManifest != null && Objects.equals(outputManifest.get("assemblyId").asText(), inputManifest.get("assemblyId").asText())) {
                System.out.println("Period: (" + period + ") has same assemblyId: (" + inputManifest.get("assemblyId").asText() + ")");
                //skip if assembly Id of this period at output bucket and input bucket equals
                continue;
            }
            // get new key to overwrite reportKeys at output bucket
            final List<String> newReportKeys = Lists.newArrayList();

            final List<String> reportKeys = StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(inputManifest.get("reportKeys").getElements(), Spliterator.ORDERED),
                    false
            )
                    .map(JsonNode::asText)
                    .collect(Collectors.toList());
            final List<String> outputPrefixes = reportKeys.stream().map(reportKey -> reportKey.substring(0, reportKey.lastIndexOf("/"))).distinct().collect(Collectors.toList());
            if (outputPrefixes.size() != 1) {
                throw new RuntimeException("Cannot extract single outputPrefix for period " + period);
            }
            newReportKeys.addAll(
                    //filter files
                    filter(
                            inputBucket,
                            outputBucket,
                            outputPrefixes.get(0),
                            reportKeys,
                            Arrays.stream(linkedAccountIdsString.split(",")).map(String::trim).collect(Collectors.toSet())
                    )
            );
            ((ObjectNode) inputManifest).putArray("reportKeys").addAll(newReportKeys.stream().map(TextNode::valueOf).collect(Collectors.toList()));
            amazonS3Client.putObject(
                    outputBucket,
                    reportPrefix + period + reportName + "-Manifest.json",
                    new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(inputManifest)),
                    null
            );
        }

    }

    public Set<String> filter(final String inputBucket, final String outputBucket, final String outputPrefix, final List<String> reportKeys, final Set<String> linkedAccountIds) {
        final SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("AWS Cost And Usage Report Filter")
                .config("parquet.writer.version", "v2")
                .config("spark.sql.parquet.binaryAsString", "true")
                .config("spark.sql.parquet.enableVectorizedReader", "false")
                .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
                .getOrCreate();
        //read parquet
        final Dataset<Row> rowDataset = spark.read().load(reportKeys.stream().map(parquetFile -> "s3a://" + inputBucket + "/" + parquetFile).collect(Collectors.toList()).toArray(new String[]{}));
        rowDataset
                //filter by linked account id
                .filter(rowDataset.col("line_item_usage_account_id").isInCollection(linkedAccountIds))
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("s3a://" + outputBucket + "/" + outputPrefix);
        spark.stop();

        String nextMarker = null;
        final Set<String> convertedKeys = Sets.newHashSet();
        do {
            final ObjectListing objectListing = amazonS3Client
                    .listObjects(
                            new ListObjectsRequest(
                                    outputBucket,
                                    outputPrefix + "/",
                                    nextMarker,
                                    "/",
                                    1000
                            )
                    );
            nextMarker = objectListing.getNextMarker();
            convertedKeys.addAll(
                    objectListing.getObjectSummaries().stream().map(o -> o.getKey()).filter(k -> !k.endsWith("_SUCCESS")).collect(Collectors.toSet())
            );
        } while (nextMarker != null);
        return convertedKeys;
    }

}
