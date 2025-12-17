package rating.engine.infrastructure.spark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class SparkConfig {

    @Bean
    public SparkSession sparkSession() {
        log.info("Creating Spark Connect session with catalog configurations...");

        SparkSession session = SparkSession.builder()
                .appName("Rating Process Engine")
                .remote("sc://localhost:15002")

                // Bronze Catalog
                .config("spark.sql.catalog.bronze", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.bronze.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
                .config("spark.sql.catalog.bronze.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.bronze.uri", "http://nessie:19120/api/v2")
                .config("spark.sql.catalog.bronze.ref", "main")
                .config("spark.sql.catalog.bronze.warehouse", "s3a://warehouse")
                .config("spark.sql.catalog.bronze.s3.access-key-id", "admin")
                .config("spark.sql.catalog.bronze.s3.secret-access-key", "password123")
                .config("spark.sql.catalog.bronze.s3.endpoint", "http://minio:9000")
                .config("spark.sql.catalog.bronze.s3.path-style-access", "true")
                .config("spark.sql.catalog.bronze.client.region", "us-east-1")

                // Silver Catalog
                .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.silver.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
                .config("spark.sql.catalog.silver.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.silver.uri", "http://nessie:19120/api/v2")
                .config("spark.sql.catalog.silver.ref", "main")
                .config("spark.sql.catalog.silver.warehouse", "s3a://warehouse")
                .config("spark.sql.catalog.silver.s3.access-key-id", "admin")
                .config("spark.sql.catalog.silver.s3.secret-access-key", "password123")
                .config("spark.sql.catalog.silver.s3.endpoint", "http://minio:9000")
                .config("spark.sql.catalog.silver.s3.path-style-access", "true")
                .config("spark.sql.catalog.silver.client.region", "us-east-1")

                // Hadoop S3 Configuration
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                .config("spark.hadoop.fs.s3a.access.key", "admin")
                .config("spark.hadoop.fs.s3a.secret.key", "password123")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

                .getOrCreate();

        log.info("Spark Connect session created successfully");
        return session;
    }


}