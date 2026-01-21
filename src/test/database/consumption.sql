CREATE TABLE silver.rating_engine.consumption
(
    metering_point_number VARCHAR,
    reading_timestamp     TIMESTAMP(6),
    consumption_value DOUBLE,
    unit                  VARCHAR,
    customer_id           VARCHAR,
    country_code          VARCHAR,
    quality_code          VARCHAR,
    processed_at          TIMESTAMP(6) WITH TIME ZONE,
    reading_date          DATE,
    reading_hour          INTEGER
)
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['country_code']
        );