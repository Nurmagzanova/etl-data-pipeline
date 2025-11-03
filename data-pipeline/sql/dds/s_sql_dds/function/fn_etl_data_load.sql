CREATE OR REPLACE FUNCTION s_sql_dds.fn_etl_data_load(
    start_date DATE DEFAULT '2023-01-01',
    end_date DATE DEFAULT '2023-12-31'
)
RETURNS INTEGER AS $$
DECLARE
    processed_count INTEGER;
BEGIN
    -- Очистка целевой таблицы в указанном диапазоне дат
    DELETE FROM s_sql_dds.t_sql_source_structured 
    WHERE effective_from >= start_date AND effective_to <= end_date;
    
    -- Вставка очищенных и трансформированных данных
    INSERT INTO s_sql_dds.t_sql_source_structured (
        user_id, user_name, age, salary, purchase_amount, product_category,
        region, customer_status, transaction_count, effective_from, effective_to, current_flag
    )
    SELECT 
        user_id,
        user_name,
        -- Очистка возраста: замена NULL, ограничение диапазона
        CASE 
            WHEN age IS NULL THEN 25
            WHEN age < 18 THEN 18
            WHEN age > 100 THEN 100
            ELSE age
        END AS age,
        -- Очистка зарплаты: замена отрицательных значений, ограничение аномалий
        CASE 
            WHEN salary < 0 THEN 0
            WHEN salary > 1000000 THEN 1000000
            ELSE ROUND(salary::NUMERIC, 2)
        END AS salary,
        -- Очистка суммы покупки: замена отрицательных, ограничение выбросов
        CASE 
            WHEN purchase_amount < 0 THEN 0
            WHEN purchase_amount > 100000 THEN 100000
            ELSE ROUND(purchase_amount::NUMERIC, 2)
        END AS purchase_amount,
        -- Очистка категорий продуктов
        CASE 
            WHEN product_category NOT IN ('Electronics', 'Clothing', 'Books', 'Home', 'Sports') 
            THEN 'Other'
            ELSE product_category
        END AS product_category,
        region,
        -- Стандартизация статусов
        CASE 
            WHEN customer_status IS NULL THEN 'unknown'
            ELSE LOWER(customer_status)
        END AS customer_status,
        -- Очистка количества транзакций
        CASE 
            WHEN transaction_count < 0 THEN 0
            WHEN transaction_count > 1000 THEN 1000
            ELSE transaction_count
        END AS transaction_count,
        -- Корректировка дат
        CASE 
            WHEN effective_from < '2020-01-01' THEN '2023-01-01'::DATE
            ELSE effective_from
        END AS effective_from,
        CASE 
            WHEN effective_to < effective_from THEN effective_from + INTERVAL '30 days'
            WHEN effective_to > '2024-12-31' THEN '2024-12-31'::DATE
            ELSE effective_to
        END AS effective_to,
        current_flag
    FROM s_sql_dds.t_sql_source_unstructured
    WHERE effective_from >= start_date 
        AND effective_to <= end_date
        AND user_id IS NOT NULL;
    
    -- Получение количества обработанных записей
    GET DIAGNOSTICS processed_count = ROW_COUNT;
    
    RETURN processed_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION s_sql_dds.fn_etl_data_load IS 'ETL функция для очистки и загрузки данных с обработкой аномалий';