CREATE TABLE IF NOT EXISTS s_sql_dds.t_sql_source_unstructured (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50),
    user_name VARCHAR(100),
    age INTEGER,
    salary NUMERIC(15,2),
    purchase_amount NUMERIC(15,2),
    product_category VARCHAR(50),
    region VARCHAR(50),
    customer_status VARCHAR(20),
    transaction_count INTEGER,
    effective_from DATE,
    effective_to DATE,
    current_flag BOOLEAN,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE s_sql_dds.t_sql_source_unstructured IS 'Неструктурированная таблица-источник с сырыми данными';