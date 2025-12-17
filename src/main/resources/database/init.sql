CREATE SCHEMA silver.rating_engine;



CREATE TABLE bronze.rating_engine.consumption_state
(
    job_name         VARCHAR,
    last_snapshot_id BIGINT,
    processed_at     VARCHAR
)
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['job_name']
        );


CREATE TABLE bronze.rating_engine.price_state
(
    job_name         VARCHAR,
    last_snapshot_id BIGINT,
    processed_at     VARCHAR
)
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['job_name']
        );


CREATE TABLE bronze.rating_engine.contract_state
(
    job_name         VARCHAR,
    last_snapshot_id BIGINT,
    processed_at     VARCHAR
)
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['job_name']
        );


CREATE TABLE bronze.rating_engine.consumption
(
    raw_json VARCHAR
)
    WITH (
        format = 'PARQUET'
        );


CREATE TABLE bronze.rating_engine.contract
(
    raw_json VARCHAR
)
    WITH (
        format = 'PARQUET'
        );


CREATE TABLE bronze.rating_engine.price
(
    raw_json VARCHAR
)
    WITH (
        format = 'PARQUET'
        );

CREATE TABLE silver.rating_engine.consumption_silver
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

CREATE TABLE silver.rating_engine.contract_silver
(
    contract_number       VARCHAR,
    customer_id           VARCHAR,
    metering_point_number VARCHAR,
    valid_from            TIMESTAMP(6),
    valid_to              TIMESTAMP(6),
    status                VARCHAR,
    product_id            VARCHAR,
    product_name          VARCHAR,
    product_type          VARCHAR,
    product_country_code  VARCHAR,
    processed_at          TIMESTAMP(6) WITH TIME ZONE
)
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['status']
        );

CREATE TABLE silver.rating_engine.price_silver
(
    price_id        VARCHAR,
    product_id      VARCHAR,
    price_component VARCHAR,
    price_per_unit DOUBLE,
    unit            VARCHAR,
    valid_from      TIMESTAMP(6),
    valid_to        TIMESTAMP(6),
    country_code    VARCHAR,
    processed_at    TIMESTAMP(6) WITH TIME ZONE
)
    WITH (
        format = 'PARQUET',
        partitioning = ARRAY['country_code']
        );