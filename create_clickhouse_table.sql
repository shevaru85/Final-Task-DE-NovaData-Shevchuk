-- Создание таблицы для российских домов в ClickHouse
CREATE TABLE IF NOT EXISTS russian_houses (
    house_id String,
    latitude Float64,
    longitude Float64,
    maintenance_year Nullable(Int32),
    square Nullable(Float64),
    population Nullable(Int32),
    region String,
    locality_name Nullable(String),
    address Nullable(String),
    full_address Nullable(String),
    communal_service_id Nullable(Float64),
    description Nullable(String)
) ENGINE = MergeTree()
ORDER BY (region, house_id);
