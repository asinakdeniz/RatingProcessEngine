package rating.engine.billingline.service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import java.time.Instant;

import static org.apache.spark.sql.functions.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessService {

    private final SparkSession spark;

    private static final String TABLE_FORMAT = "iceberg";

    // Consumption tables
    private static final String CONSUMPTION_BRONZE = "bronze.rating_engine.consumption";
    private static final String CONSUMPTION_SILVER = "silver.rating_engine.consumption_silver";
    private static final String CONSUMPTION_STATE = "bronze.rating_engine.consumption_state";

    // Contract tables
    private static final String CONTRACT_BRONZE = "bronze.rating_engine.contract";
    private static final String CONTRACT_SILVER = "silver.rating_engine.contract_silver";
    private static final String CONTRACT_STATE = "bronze.rating_engine.contract_state";

    // Price tables
    private static final String PRICE_BRONZE = "bronze.rating_engine.price";
    private static final String PRICE_SILVER = "silver.rating_engine.price_silver";
    private static final String PRICE_STATE = "bronze.rating_engine.price_state";

    /**
     * Transform all entities: consumption, contract, and price
     */
    public void transformAll() {
        log.info("Starting transformation of all records");
        StopWatch stopWatch = new StopWatch("Transform All");
        stopWatch.start();

        transformPriceData();
        transformContractData();
        transformConsumptionData();

        stopWatch.stop();
        log.info("All transformations completed {}", stopWatch.prettyPrint());
    }

    /**
     * Transform consumption data from bronze to silver
     */
    public void transformConsumptionData() {
        StopWatch stopWatch = new StopWatch("ConsumptionTransformation");
        log.info("Starting Consumption Bronze to Silver transformation");

        try {
            stopWatch.start("Get Consumption Snapshots");
            Long currentSnapshotId = getCurrentSnapshotId(CONSUMPTION_BRONZE);
            Long lastProcessedSnapshotId = getLastProcessedSnapshotId(CONSUMPTION_STATE, "consumption_transform");
            stopWatch.stop();

            if (currentSnapshotId == null) {
                log.warn("No snapshots found in consumption bronze table");
                return;
            }

            if (currentSnapshotId.equals(lastProcessedSnapshotId)) {
                log.info("No new consumption data to process. Current snapshot: {}", currentSnapshotId);
                return;
            }

            log.info("Processing consumption data from snapshot {} to {}", lastProcessedSnapshotId, currentSnapshotId);

            stopWatch.start("Read Consumption Incremental Data");
            Dataset<Row> incrementalDF = readIncrementalData(CONSUMPTION_BRONZE, lastProcessedSnapshotId, currentSnapshotId);
            stopWatch.stop();

            stopWatch.start("Transform Consumption Data to Silver");
            Dataset<Row> silverDF = transformConsumptionToSilver(incrementalDF);
            stopWatch.stop();

            stopWatch.start("Write Consumption Data to Silver");
            silverDF.write()
                    .format(TABLE_FORMAT)
                    .mode("append")
                    .partitionBy("country_code")
                    .save(CONSUMPTION_SILVER);
            stopWatch.stop();

            stopWatch.start("Update Consumption State");
            updateProcessingState(CONSUMPTION_STATE, "consumption_transform", currentSnapshotId);
            stopWatch.stop();

            log.info("Consumption transformation completed successfully\n{}", stopWatch.prettyPrint());

        } catch (Exception e) {
            log.error("Error during Consumption transformation", e);
            throw new RuntimeException("Consumption transformation failed", e);
        }
    }

    /**
     * Transform contract data from bronze to silver
     */
    public void transformContractData() {
        StopWatch stopWatch = new StopWatch("ContractTransformation");
        log.info("Starting Contract Bronze to Silver transformation");

        try {
            stopWatch.start("Get Contract Snapshots");
            Long currentSnapshotId = getCurrentSnapshotId(CONTRACT_BRONZE);
            Long lastProcessedSnapshotId = getLastProcessedSnapshotId(CONTRACT_STATE, "contract_transform");
            stopWatch.stop();

            if (currentSnapshotId == null) {
                log.warn("No snapshots found in contract bronze table");
                return;
            }

            if (currentSnapshotId.equals(lastProcessedSnapshotId)) {
                log.info("No new contract data to process. Current snapshot: {}", currentSnapshotId);
                return;
            }

            log.info("Processing contract data from snapshot {} to {}", lastProcessedSnapshotId, currentSnapshotId);

            stopWatch.start("Read Contract Incremental Data");
            Dataset<Row> incrementalDF = readIncrementalData(CONTRACT_BRONZE, lastProcessedSnapshotId, currentSnapshotId);
            stopWatch.stop();

            stopWatch.start("Transform Contract Data to Silver");
            Dataset<Row> silverDF = transformContractToSilver(incrementalDF);
            stopWatch.stop();

            stopWatch.start("Write Contract Data to Silver");
            silverDF.write()
                    .format(TABLE_FORMAT)
                    .mode("append")
                    .partitionBy("status")
                    .save(CONTRACT_SILVER);
            stopWatch.stop();

            stopWatch.start("Update Contract State");
            updateProcessingState(CONTRACT_STATE, "contract_transform", currentSnapshotId);
            stopWatch.stop();

            log.info("Contract transformation completed successfully");
            log.info("Timing breakdown:\n{}", stopWatch.prettyPrint());

        } catch (Exception e) {
            log.error("Error during Contract transformation", e);
            throw new RuntimeException("Contract transformation failed", e);
        }
    }

    /**
     * Transform price data from bronze to silver
     */
    public void transformPriceData() {
        StopWatch stopWatch = new StopWatch("PriceTransformation");
        log.info("Starting Price Bronze to Silver transformation");

        try {
            stopWatch.start("Get Price Snapshots");
            Long currentSnapshotId = getCurrentSnapshotId(PRICE_BRONZE);
            Long lastProcessedSnapshotId = getLastProcessedSnapshotId(PRICE_STATE, "price_transform");
            stopWatch.stop();

            if (currentSnapshotId == null) {
                log.warn("No snapshots found in price bronze table");
                return;
            }

            if (currentSnapshotId.equals(lastProcessedSnapshotId)) {
                log.info("No new price data to process. Current snapshot: {}", currentSnapshotId);
                return;
            }

            log.info("Processing price data from snapshot {} to {}", lastProcessedSnapshotId, currentSnapshotId);

            stopWatch.start("Read Price Incremental Data");
            Dataset<Row> incrementalDF = readIncrementalData(PRICE_BRONZE, lastProcessedSnapshotId, currentSnapshotId);
            stopWatch.stop();

            stopWatch.start("Transform Price Data to Silver");
            Dataset<Row> silverDF = transformPriceToSilver(incrementalDF);
            stopWatch.stop();

            stopWatch.start("Write Price Data to Silver");
            silverDF.write()
                    .format(TABLE_FORMAT)
                    .mode("append")
                    .partitionBy("country_code")
                    .save(PRICE_SILVER);
            stopWatch.stop();

            stopWatch.start("Update State");
            updateProcessingState(PRICE_STATE, "price_transform", currentSnapshotId);
            stopWatch.stop();

            log.info("Price transformation completed successfully");
            log.info("Timing breakdown:\n{}", stopWatch.prettyPrint());

        } catch (Exception e) {
            log.error("Error during Price transformation", e);
            throw new RuntimeException("Price transformation failed", e);
        }
    }

    /**
     * Transform consumption bronze data to silver
     */
    private Dataset<Row> transformConsumptionToSilver(Dataset<Row> bronzeDF) {
        log.info("Transforming consumption data to silver layer");

        StructType consumptionSchema = new StructType()
                .add("meteringPointNumber", DataTypes.StringType)
                .add("timestamp", DataTypes.StringType)
                .add("consumption", DataTypes.createDecimalType(18, 2))
                .add("unit", DataTypes.StringType)
                .add("customerId", DataTypes.StringType)
                .add("countryCode", DataTypes.StringType)
                .add("qualityCode", DataTypes.StringType);

        return bronzeDF
                .withColumn("parsed", from_json(col("raw_json"), consumptionSchema))
                .select(
                        col("parsed.meteringPointNumber").alias("metering_point_number"),
                        to_timestamp(col("parsed.timestamp")).alias("reading_timestamp"),
                        col("parsed.consumption").cast(DataTypes.DoubleType).alias("consumption_value"),
                        col("parsed.unit").alias("unit"),
                        col("parsed.customerId").alias("customer_id"),
                        col("parsed.countryCode").alias("country_code"),
                        col("parsed.qualityCode").alias("quality_code"),
                        current_timestamp().alias("processed_at")
                )
                .filter(col("metering_point_number").isNotNull())
                .filter(col("reading_timestamp").isNotNull())
                .filter(col("consumption_value").isNotNull())
                .filter(col("consumption_value").geq(0))
                .filter(col("quality_code").equalTo("VALID"))
                .withColumn("reading_date", to_date(col("reading_timestamp")))
                .withColumn("reading_hour", hour(col("reading_timestamp")))
                .repartition(col("country_code"));
    }

    /**
     * Transform contract bronze data to silver
     */
    private Dataset<Row> transformContractToSilver(Dataset<Row> bronzeDF) {
        log.info("Transforming contract data to silver layer");

        StructType productSchema = new StructType()
                .add("productId", DataTypes.StringType)
                .add("productName", DataTypes.StringType)
                .add("productType", DataTypes.StringType)
                .add("countryCode", DataTypes.StringType);

        StructType contractSchema = new StructType()
                .add("contractNumber", DataTypes.StringType)
                .add("customerId", DataTypes.StringType)
                .add("meteringPointNumber", DataTypes.StringType)
                .add("validFrom", DataTypes.StringType)
                .add("validTo", DataTypes.StringType)
                .add("status", DataTypes.StringType)
                .add("product", productSchema);

        return bronzeDF
                .withColumn("parsed", from_json(col("raw_json"), contractSchema))
                .select(
                        col("parsed.contractNumber").alias("contract_number"),
                        col("parsed.customerId").alias("customer_id"),
                        col("parsed.meteringPointNumber").alias("metering_point_number"),
                        to_timestamp(col("parsed.validFrom")).alias("valid_from"),
                        to_timestamp(col("parsed.validTo")).alias("valid_to"),
                        col("parsed.status").alias("status"),
                        col("parsed.product.productId").alias("product_id"),
                        col("parsed.product.productName").alias("product_name"),
                        col("parsed.product.productType").alias("product_type"),
                        col("parsed.product.countryCode").alias("product_country_code"),
                        current_timestamp().alias("processed_at")
                )
                .filter(col("contract_number").isNotNull())
                .filter(col("metering_point_number").isNotNull())
                .filter(col("valid_from").isNotNull())
                .filter(col("product_id").isNotNull())
                .repartition(col("status"));
    }

    /**
     * Transform price bronze data to silver
     */
    private Dataset<Row> transformPriceToSilver(Dataset<Row> bronzeDF) {
        log.info("Transforming price data to silver layer");

        StructType priceSchema = new StructType()
                .add("priceId", DataTypes.StringType)
                .add("productId", DataTypes.StringType)
                .add("priceComponent", DataTypes.StringType)
                .add("pricePerUnit", DataTypes.createDecimalType(18, 4))
                .add("unit", DataTypes.StringType)
                .add("validFrom", DataTypes.StringType)
                .add("validTo", DataTypes.StringType)
                .add("countryCode", DataTypes.StringType);

        return bronzeDF
                .withColumn("parsed", from_json(col("raw_json"), priceSchema))
                .select(
                        col("parsed.priceId").alias("price_id"),
                        col("parsed.productId").alias("product_id"),
                        col("parsed.priceComponent").alias("price_component"),
                        col("parsed.pricePerUnit").cast(DataTypes.DoubleType).alias("price_per_unit"),
                        col("parsed.unit").alias("unit"),
                        to_timestamp(col("parsed.validFrom")).alias("valid_from"),
                        to_timestamp(col("parsed.validTo")).alias("valid_to"),
                        col("parsed.countryCode").alias("country_code"),
                        current_timestamp().alias("processed_at")
                )
                .filter(col("price_id").isNotNull())
                .filter(col("product_id").isNotNull())
                .filter(col("price_per_unit").isNotNull())
                .filter(col("price_per_unit").gt(0))
                .filter(col("valid_from").isNotNull())
                .repartition(col("country_code"));
    }

    /**
     * Read incremental data between two snapshots
     */
    private Dataset<Row> readIncrementalData(String bronzeTable, Long startSnapshotId, Long endSnapshotId) {
        if (startSnapshotId == null) {
            log.info("First run detected, reading full snapshot: {}", endSnapshotId);
            return spark.read()
                    .format(TABLE_FORMAT)
                    .option("snapshot-id", endSnapshotId.toString())
                    .load(bronzeTable);
        } else {
            log.info("Reading incremental changes between snapshots {} and {}", startSnapshotId, endSnapshotId);
            return spark.read()
                    .format(TABLE_FORMAT)
                    .option("start-snapshot-id", startSnapshotId.toString())
                    .option("end-snapshot-id", endSnapshotId.toString())
                    .load(bronzeTable);
        }
    }

    /**
     * Get current snapshot ID from bronze table
     */
    private Long getCurrentSnapshotId(String bronzeTable) {
        try {
            Dataset<Row> snapshotDF = spark.sql(
                    String.format("SELECT snapshot_id FROM %s.snapshots ORDER BY committed_at DESC LIMIT 1", bronzeTable)
            );

            if (snapshotDF.isEmpty()) {
                return null;
            }

            Long snapshotId = snapshotDF.first().getLong(0);
            log.info("Current snapshot ID for {}: {}", bronzeTable, snapshotId);
            return snapshotId;
        } catch (Exception e) {
            log.warn("Error getting snapshot for {}: {}", bronzeTable, e.getMessage());
            return null;
        }
    }

    /**
     * Get last processed snapshot ID from state table
     */
    private Long getLastProcessedSnapshotId(String stateTable, String jobName) {
        try {
            Dataset<Row> stateDF = spark.sql(
                    String.format(
                            "SELECT last_snapshot_id FROM %s WHERE job_name = '%s' ORDER BY processed_at DESC LIMIT 1",
                            stateTable, jobName
                    )
            );

            if (stateDF.isEmpty()) {
                log.info("No previous processing state found for {} - first run", jobName);
                return null;
            }

            Long lastSnapshotId = stateDF.first().getLong(0);
            log.info("Last processed snapshot ID for {}: {}", jobName, lastSnapshotId);
            return lastSnapshotId;

        } catch (Exception e) {
            log.warn("Error reading state table {} (may not exist yet): {}", stateTable, e.getMessage());
            return null;
        }
    }

    /**
     * Update processing state
     */
    private void updateProcessingState(String stateTable, String jobName, Long snapshotId) {
        try {
            Dataset<Row> stateDF = spark.createDataFrame(
                    java.util.List.of(new ProcessingState(jobName, snapshotId, Instant.now().toString())),
                    ProcessingState.class
            );

            stateDF.write()
                    .format(TABLE_FORMAT)
                    .mode("append")
                    .save(stateTable);

            log.info("Updated processing state for {} with snapshot ID: {}", jobName, snapshotId);

        } catch (Exception e) {
            log.error("Failed to update processing state for {}", jobName, e);
            log.warn("Continuing despite state update failure");
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ProcessingState {
        private String job_name;
        private Long last_snapshot_id;
        private String processed_at;
    }
}