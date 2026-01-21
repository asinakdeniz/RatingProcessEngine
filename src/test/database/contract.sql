CREATE TABLE silver.rating_engine.contract
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
        format = 'PARQUET'
        );