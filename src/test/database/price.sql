CREATE TABLE silver.rating_engine.price
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