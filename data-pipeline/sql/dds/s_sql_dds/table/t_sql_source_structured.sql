CREATE SCHEMA IF NOT EXISTS s_sql_dds;

CREATE TABLE IF NOT EXISTS s_sql_dds.t_sql_source_structured (
    id SERIAL PRIMARY KEY,
    age INT,
    salary NUMERIC,
    purchase_amount NUMERIC,
    gender VARCHAR(10),
    region VARCHAR(10),
    product_category VARCHAR(20),
    payment_method VARCHAR(10),
    transaction_date DATE
);CREATE TABLE IF NOT EXISTS s_sql_dds.t_sql_source_structured (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    user_name VARCHAR(100),
    age INTEGER CHECK (age BETWEEN 18 AND 100),
    salary NUMERIC(15,2) CHECK (salary >= 0),
    purchase_amount NUMERIC(15,2) CHECK (purchase_amount >= 0 AND purchase_amount <= 100000),
    product_category VARCHAR(50) CHECK (product_category IN ('Electronics', 'Clothing', 'Books', 'Home', 'Sports')),
    region VARCHAR(50),
    customer_status VARCHAR(20),
    transaction_count INTEGER CHECK (transaction_count >= 0),
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    current_flag BOOLEAN,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_date_range CHECK (effective_to >= effective_from)
);

CREATE INDEX IF NOT EXISTS idx_structured_user_id ON s_sql_dds.t_sql_source_structured(user_id);
CREATE INDEX IF NOT EXISTS idx_structured_dates ON s_sql_dds.t_sql_source_structured(effective_from, effective_to);

COMMENT ON TABLE s_sql_dds.t_sql_source_structured IS 'Структурированная таблица с очищенными данными';