CREATE OR REPLACE FUNCTION s_sql_dds.fn_etl_data_load_test(
    start_date DATE DEFAULT '2023-01-01',
    end_date DATE DEFAULT '2023-12-31'
)
RETURNS INTEGER AS $$
DECLARE
    test_count INTEGER;
BEGIN
    -- Создание тестовой таблицы если не существует
    CREATE TABLE IF NOT EXISTS s_sql_dds.t_sql_source_structured_copy (
        LIKE s_sql_dds.t_sql_source_structured INCLUDING ALL
    );
    
    -- Очистка тестовой таблицы
    DELETE FROM s_sql_dds.t_sql_source_structured_copy 
    WHERE effective_from >= start_date AND effective_to <= end_date;
    
    -- Вставка данных в тестовую таблицу
    INSERT INTO s_sql_dds.t_sql_source_structured_copy 
    SELECT * FROM s_sql_dds.t_sql_source_structured
    WHERE effective_from >= start_date AND effective_to <= end_date;
    
    GET DIAGNOSTICS test_count = ROW_COUNT;
    
    RETURN test_count;
END;
$$ LANGUAGE plpgsql;