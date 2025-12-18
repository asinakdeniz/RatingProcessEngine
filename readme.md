# Rating Process Engine

## Architecture Overview

The Rating Process Engine implements a medallion architecture (Bronze → Silver → Gold) with the following key architectural patterns:

**Incremental Processing:**

- Uses Apache Iceberg snapshots for incremental data processing
- State tables track last processed snapshot ID to avoid reprocessing
- Only reads and transforms new data since last run

**Separation of Concerns:**

- Spring Boot application acts as orchestrator (thin client via Spark Connect)
- Spark cluster handles heavy distributed processing
- MinIO provides S3-compatible object storage
- Nessie manages Iceberg catalog with version control

**Data Quality & Validation:**

- JSON schema validation during parsing
- Null checks and data cleansing filters
- Partitioning by country_code (consumption/price) and status (contracts)

### Spark Connect Architecture

With Spark Connect:

```
Rating Process Engine (thin client) → gRPC calls → Spark Connect Server (Driver)
                                                        ↓
                                                    Spark Master
                                                        ↓
                                                    Workers (executors)
```

How it works:

1. Spark Connect Server runs as a long-lived process with the Spark driver embedded
2. Rating Process Engine becomes a thin client - just sends commands over gRPC
3. The server translates your commands into actual Spark operations
4. Results stream back to your client over gRPC

Key benefits

* No fat Spark JARs in your microservice - just a lightweight client library
* Decoupled lifecycle - Rating Process Engine can restart without killing Spark jobs
* Multiple clients can connect to the same Spark cluster simultaneously

## Database Initialization

The project includes `resources/database/init.sql` with DDL scripts to create Bronze and Silver Iceberg tables:

- Bronze tables: consumption, contract, price (for raw JSON ingestion)
- Silver tables: consumption_silver, contract_silver, price_silver (for structured data)
- State tables: consumption_state, contract_state, price_state (for tracking processing progress)

These tables are automatically created through Trino/Spark when the infrastructure starts up.

## Manual IntelliJ Configuration

1. **Run → Edit Configurations**
2. Select your Spring Boot app
3. **VM options** field, paste this ONE line:

```
--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
```

4. **Apply** and **OK**

---