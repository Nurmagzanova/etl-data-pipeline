import psycopg2
import sys
import os

# Добавляем путь для импорта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import DB_CONFIG
except ImportError:
    # Альтернативный способ если импорт не работает
    DB_CONFIG = {
        'host': 'localhost',
        'port': '5432',
        'database': 'etl_db',
        'user': 'user',
        'password': 'password'
    }

def init_database():
    """
    Инициализация базы данных - создание схемы, таблиц и функций
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        print("Инициализация базы данных...")
        
        # Создание схемы
        print("Создание схемы s_sql_dds...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS s_sql_dds;")
        
        # Создание неструктурированной таблицы
        print("Создание таблицы t_sql_source_unstructured...")
        cur.execute("""
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
        """)
        
        # Создание структурированной таблицы
        print("Создание таблицы t_sql_source_structured...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS s_sql_dds.t_sql_source_structured (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
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
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Создание ТЕСТОВОЙ таблицы
        print("Создание тестовой таблицы t_sql_source_structured_copy...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS s_sql_dds.t_sql_source_structured_copy (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
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
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Создание ETL функции
        print("Создание функции fn_etl_data_load...")
        cur.execute("""
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
                    -- Очистка возраста
                    CASE 
                        WHEN age IS NULL THEN 25
                        WHEN age < 18 THEN 18
                        WHEN age > 100 THEN 100
                        ELSE age
                    END AS age,
                    -- Очистка зарплаты
                    CASE 
                        WHEN salary < 0 THEN 0
                        WHEN salary > 1000000 THEN 1000000
                        ELSE ROUND(salary::NUMERIC, 2)
                    END AS salary,
                    -- Очистка суммы покупки
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
        """)
        
        # Создание ТЕСТОВОЙ ETL функции
        print("Создание тестовой функции fn_etl_data_load_test...")
        cur.execute("""
            CREATE OR REPLACE FUNCTION s_sql_dds.fn_etl_data_load_test(
                start_date DATE DEFAULT '2023-01-01',
                end_date DATE DEFAULT '2023-12-31'
            )
            RETURNS INTEGER AS $$
            DECLARE
                test_count INTEGER;
            BEGIN
                -- Очистка тестовой таблицы в указанном диапазоне дат
                DELETE FROM s_sql_dds.t_sql_source_structured_copy 
                WHERE effective_from >= start_date AND effective_to <= end_date;
                
                -- Копирование данных из структурированной таблицы в тестовую
                INSERT INTO s_sql_dds.t_sql_source_structured_copy 
                SELECT * FROM s_sql_dds.t_sql_source_structured
                WHERE effective_from >= start_date AND effective_to <= end_date;
                
                -- Получение количества скопированных записей
                GET DIAGNOSTICS test_count = ROW_COUNT;
                
                RETURN test_count;
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        # ========== ДОБАВЛЯЕМ DWH ТАБЛИЦЫ И ФУНКЦИИ ==========
        
        print("Создание DWH таблиц...")
        
        # Справочник клиентов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS s_sql_dds.t_dim_customer (
                customer_id SERIAL PRIMARY KEY,
                customer_name VARCHAR(255) UNIQUE NOT NULL,
                created_dt DATE DEFAULT CURRENT_DATE
            );
        """)
        
        # Справочник продуктов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS s_sql_dds.t_dim_product (
                product_id SERIAL PRIMARY KEY,
                product_category VARCHAR(100) UNIQUE NOT NULL,
                created_dt DATE DEFAULT CURRENT_DATE
            );
        """)
        
        # Справочник регионов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS s_sql_dds.t_dim_region (
                region_id SERIAL PRIMARY KEY,
                region_name VARCHAR(100) UNIQUE NOT NULL,
                created_dt DATE DEFAULT CURRENT_DATE
            );
        """)
        
        # Справочник статусов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS s_sql_dds.t_dim_status (
                status_id SERIAL PRIMARY KEY,
                status_name VARCHAR(50) UNIQUE NOT NULL,
                created_dt DATE DEFAULT CURRENT_DATE
            );
        """)
        
        # Фактовая таблица DWH
        cur.execute("""
            CREATE TABLE IF NOT EXISTS s_sql_dds.t_dm_task (
                fact_id BIGSERIAL PRIMARY KEY,
                customer_id INT REFERENCES s_sql_dds.t_dim_customer(customer_id),
                product_id INT REFERENCES s_sql_dds.t_dim_product(product_id),
                region_id INT REFERENCES s_sql_dds.t_dim_region(region_id),
                status_id INT REFERENCES s_sql_dds.t_dim_status(status_id),
                age INTEGER,
                salary NUMERIC(15,2),
                purchase_amount NUMERIC(15,2),
                transaction_count INTEGER,
                effective_from DATE,
                effective_to DATE,
                current_flag BOOLEAN,
                created_dt DATE DEFAULT CURRENT_DATE
            );
        """)
        
        # Функция загрузки данных в DWH (ИСПРАВЛЕННАЯ)
        print("Создание функции fn_dm_data_load...")
        cur.execute("""
            CREATE OR REPLACE FUNCTION s_sql_dds.fn_dm_data_load(
                start_dt DATE DEFAULT NULL,
                end_dt DATE DEFAULT NULL
            )
            RETURNS VOID AS $$
            BEGIN
                -- Вставка данных в справочник клиентов
                INSERT INTO s_sql_dds.t_dim_customer (customer_name)
                SELECT DISTINCT user_name 
                FROM s_sql_dds.t_sql_source_structured
                WHERE user_name IS NOT NULL
                  AND (start_dt IS NULL OR effective_from >= start_dt)
                  AND (end_dt IS NULL OR effective_to <= end_dt)
                ON CONFLICT (customer_name) DO NOTHING;

                -- Вставка данных в справочник продуктов
                INSERT INTO s_sql_dds.t_dim_product (product_category)
                SELECT DISTINCT product_category 
                FROM s_sql_dds.t_sql_source_structured
                WHERE product_category IS NOT NULL
                  AND (start_dt IS NULL OR effective_from >= start_dt)
                  AND (end_dt IS NULL OR effective_to <= end_dt)
                ON CONFLICT (product_category) DO NOTHING;

                -- Вставка данных в справочник регионов
                INSERT INTO s_sql_dds.t_dim_region (region_name)
                SELECT DISTINCT region 
                FROM s_sql_dds.t_sql_source_structured
                WHERE region IS NOT NULL
                  AND (start_dt IS NULL OR effective_from >= start_dt)
                  AND (end_dt IS NULL OR effective_to <= end_dt)
                ON CONFLICT (region_name) DO NOTHING;

                -- Вставка данных в справочник статусов
                INSERT INTO s_sql_dds.t_dim_status (status_name)
                SELECT DISTINCT customer_status 
                FROM s_sql_dds.t_sql_source_structured
                WHERE customer_status IS NOT NULL
                  AND (start_dt IS NULL OR effective_from >= start_dt)
                  AND (end_dt IS NULL OR effective_to <= end_dt)
                ON CONFLICT (status_name) DO NOTHING;

                -- Вставка данных в фактовую таблицу
                INSERT INTO s_sql_dds.t_dm_task (
                    customer_id,
                    product_id,
                    region_id,
                    status_id,
                    age,
                    salary,
                    purchase_amount,
                    transaction_count,
                    effective_from,
                    effective_to,
                    current_flag
                )
                SELECT 
                    c.customer_id,
                    p.product_id,
                    r.region_id,
                    st.status_id,
                    src.age,
                    src.salary,
                    src.purchase_amount,
                    src.transaction_count,
                    src.effective_from,
                    src.effective_to,
                    src.current_flag
                FROM s_sql_dds.t_sql_source_structured src
                LEFT JOIN s_sql_dds.t_dim_customer c ON src.user_name = c.customer_name
                LEFT JOIN s_sql_dds.t_dim_product p ON src.product_category = p.product_category
                LEFT JOIN s_sql_dds.t_dim_region r ON src.region = r.region_name
                LEFT JOIN s_sql_dds.t_dim_status st ON src.customer_status = st.status_name
                WHERE (start_dt IS NULL OR src.effective_from >= start_dt)
                  AND (end_dt IS NULL OR src.effective_to <= end_dt);

            END;
            $$ LANGUAGE plpgsql;
        """)
        
        # Создание представления
        print("Создание представления v_dm_task...")
        cur.execute("""
            CREATE OR REPLACE VIEW s_sql_dds.v_dm_task AS
            SELECT 
                fact_id,
                customer_id,
                product_id,
                region_id,
                status_id,
                age,
                salary,
                purchase_amount,
                transaction_count,
                effective_from,
                effective_to,
                current_flag,
                created_dt
            FROM s_sql_dds.t_dm_task;
        """)
        
        # ========== ДОБАВЛЯЕМ DATA QUALITY КОМПОНЕНТЫ ==========
        
        print("Создание Data Quality таблиц...")
        
        # Таблица для результатов проверок качества данных
        cur.execute("""
            CREATE TABLE IF NOT EXISTS s_sql_dds.t_dq_check_results (
                check_id SERIAL PRIMARY KEY,
                check_type VARCHAR(50),
                table_name VARCHAR(100),
                column_name VARCHAR(100),
                check_name VARCHAR(200),
                execution_date TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20),
                expected_value NUMERIC,
                actual_value NUMERIC,
                error_threshold NUMERIC,
                error_message VARCHAR(500)
            );
        """)
        
        # Создание индексов для таблицы DQ проверок
        print("Создание индексов для DQ таблиц...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dq_check_date 
            ON s_sql_dds.t_dq_check_results(execution_date);
            
            CREATE INDEX IF NOT EXISTS idx_dq_check_status 
            ON s_sql_dds.t_dq_check_results(status);
            
            CREATE INDEX IF NOT EXISTS idx_dq_check_type 
            ON s_sql_dds.t_dq_check_results(check_type);
        """)
        
        # Создание функции проверки качества данных (УПРОЩЕННАЯ версия)
        print("Создание функции fn_dq_checks_load...")
        cur.execute("""
            CREATE OR REPLACE FUNCTION s_sql_dds.fn_dq_checks_load(
                start_dt DATE DEFAULT NULL,
                end_dt DATE DEFAULT NULL
            )
            RETURNS VOID AS $$
            DECLARE
                v_check_count INTEGER := 0;
                v_passed_count INTEGER := 0;
                v_failed_count INTEGER := 0;
                v_error_message VARCHAR;
                v_expected NUMERIC;
                v_actual NUMERIC;
            BEGIN
                -- Очищаем предыдущие результаты за период
                DELETE FROM s_sql_dds.t_dq_check_results 
                WHERE execution_date::DATE >= COALESCE(start_dt, '1900-01-01'::DATE)
                  AND execution_date::DATE <= COALESCE(end_dt, '2100-12-31'::DATE);
                
                -- 1. Проверка правильности: Сравнение суммы покупок
                BEGIN
                    v_check_count := v_check_count + 1;
                    
                    SELECT COALESCE(SUM(purchase_amount), 0) INTO v_expected
                    FROM s_sql_dds.t_sql_source_structured
                    WHERE (start_dt IS NULL OR effective_from >= start_dt)
                      AND (end_dt IS NULL OR effective_to <= end_dt);
                    
                    SELECT COALESCE(SUM(purchase_amount), 0) INTO v_actual
                    FROM s_sql_dds.t_dm_task
                    WHERE (start_dt IS NULL OR effective_from >= start_dt)
                      AND (end_dt IS NULL OR effective_to <= end_dt);
                    
                    IF ABS(v_expected - v_actual) <= 0.01 OR v_expected = 0 THEN
                        v_passed_count := v_passed_count + 1;
                        INSERT INTO s_sql_dds.t_dq_check_results 
                        (check_type, table_name, check_name, status, expected_value, actual_value, error_threshold, error_message)
                        VALUES ('correctness', 't_dm_task', 'Purchase amount sum comparison', 'passed', 
                                v_expected, v_actual, 0.01, 'Sum difference within acceptable range');
                    ELSE
                        v_failed_count := v_failed_count + 1;
                        INSERT INTO s_sql_dds.t_dq_check_results 
                        (check_type, table_name, check_name, status, expected_value, actual_value, error_threshold, error_message)
                        VALUES ('correctness', 't_dm_task', 'Purchase amount sum comparison', 'failed', 
                                v_expected, v_actual, 0.01, 'Sum difference exceeds threshold');
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    v_failed_count := v_failed_count + 1;
                    INSERT INTO s_sql_dds.t_dq_check_results 
                    (check_type, table_name, check_name, status, error_message)
                    VALUES ('correctness', 't_dm_task', 'Purchase amount sum comparison', 'error', 
                            'Error: ' || SQLERRM);
                END;
                
                -- 2. Проверка полноты: Процент пропусков в customer_id
                BEGIN
                    v_check_count := v_check_count + 1;
                    
                    SELECT 
                        COUNT(*) FILTER (WHERE customer_id IS NULL) * 100.0 / NULLIF(COUNT(*), 0)
                    INTO v_actual
                    FROM s_sql_dds.t_dm_task
                    WHERE (start_dt IS NULL OR effective_from >= start_dt)
                      AND (end_dt IS NULL OR effective_to <= end_dt);
                    
                    IF COALESCE(v_actual, 0) <= 5 THEN
                        v_passed_count := v_passed_count + 1;
                        INSERT INTO s_sql_dds.t_dq_check_results 
                        (check_type, table_name, column_name, check_name, status, actual_value, error_threshold, error_message)
                        VALUES ('completeness', 't_dm_task', 'customer_id', 'Null values percentage', 'passed', 
                                v_actual, 5, 'Null values within acceptable range');
                    ELSE
                        v_failed_count := v_failed_count + 1;
                        INSERT INTO s_sql_dds.t_dq_check_results 
                        (check_type, table_name, column_name, check_name, status, actual_value, error_threshold, error_message)
                        VALUES ('completeness', 't_dm_task', 'customer_id', 'Null values percentage', 'failed', 
                                v_actual, 5, 'Too many null values');
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    v_failed_count := v_failed_count + 1;
                    INSERT INTO s_sql_dds.t_dq_check_results 
                    (check_type, table_name, check_name, status, error_message)
                    VALUES ('completeness', 't_dm_task', 'Null values check', 'error', 
                            'Error: ' || SQLERRM);
                END;
                
                -- 3. Проверка непротиворечивости: Дата окончания >= даты начала
                BEGIN
                    v_check_count := v_check_count + 1;
                    
                    SELECT COUNT(*) INTO v_actual
                    FROM s_sql_dds.t_dm_task
                    WHERE effective_to < effective_from
                      AND (start_dt IS NULL OR effective_from >= start_dt)
                      AND (end_dt IS NULL OR effective_to <= end_dt);
                    
                    IF v_actual = 0 THEN
                        v_passed_count := v_passed_count + 1;
                        INSERT INTO s_sql_dds.t_dq_check_results 
                        (check_type, table_name, check_name, status, actual_value, error_threshold, error_message)
                        VALUES ('consistency', 't_dm_task', 'Date range validation', 'passed', 
                                v_actual, 0, 'All date ranges are valid');
                    ELSE
                        v_failed_count := v_failed_count + 1;
                        INSERT INTO s_sql_dds.t_dq_check_results 
                        (check_type, table_name, check_name, status, actual_value, error_threshold, error_message)
                        VALUES ('consistency', 't_dm_task', 'Date range validation', 'failed', 
                                v_actual, 0, 'Found invalid date ranges');
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    v_failed_count := v_failed_count + 1;
                    INSERT INTO s_sql_dds.t_dq_check_results 
                    (check_type, table_name, check_name, status, error_message)
                    VALUES ('consistency', 't_dm_task', 'Date range validation', 'error', 
                            'Error: ' || SQLERRM);
                END;
                
                -- 4. Проверка уникальности: Отсутствие дубликатов
                BEGIN
                    v_check_count := v_check_count + 1;
                    
                    WITH duplicates AS (
                        SELECT customer_id, product_id, region_id, effective_from,
                               COUNT(*) as duplicate_count
                        FROM s_sql_dds.t_dm_task
                        WHERE (start_dt IS NULL OR effective_from >= start_dt)
                          AND (end_dt IS NULL OR effective_to <= end_dt)
                        GROUP BY customer_id, product_id, region_id, effective_from
                        HAVING COUNT(*) > 1
                    )
                    SELECT COUNT(*) INTO v_actual FROM duplicates;
                    
                    IF v_actual = 0 THEN
                        v_passed_count := v_passed_count + 1;
                        INSERT INTO s_sql_dds.t_dq_check_results 
                        (check_type, table_name, check_name, status, actual_value, error_threshold, error_message)
                        VALUES ('uniqueness', 't_dm_task', 'Duplicate records check', 'passed', 
                                v_actual, 0, 'No duplicate records found');
                    ELSE
                        v_failed_count := v_failed_count + 1;
                        INSERT INTO s_sql_dds.t_dq_check_results 
                        (check_type, table_name, check_name, status, actual_value, error_threshold, error_message)
                        VALUES ('uniqueness', 't_dm_task', 'Duplicate records check', 'failed', 
                                v_actual, 0, 'Found duplicate records');
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    v_failed_count := v_failed_count + 1;
                    INSERT INTO s_sql_dds.t_dq_check_results 
                    (check_type, table_name, check_name, status, error_message)
                    VALUES ('uniqueness', 't_dm_task', 'Duplicate check', 'error', 
                            'Error: ' || SQLERRM);
                END;
                
                -- 5. Проверка валидности: Диапазон значений salary
                BEGIN
                    v_check_count := v_check_count + 1;
                    
                    SELECT COUNT(*) INTO v_actual
                    FROM s_sql_dds.t_dm_task
                    WHERE (salary < 0 OR salary > 1000000)
                      AND (start_dt IS NULL OR effective_from >= start_dt)
                      AND (end_dt IS NULL OR effective_to <= end_dt);
                    
                    IF v_actual = 0 THEN
                        v_passed_count := v_passed_count + 1;
                        INSERT INTO s_sql_dds.t_dq_check_results 
                        (check_type, table_name, column_name, check_name, status, actual_value, error_threshold, error_message)
                        VALUES ('validity', 't_dm_task', 'salary', 'Salary range validation', 'passed', 
                                v_actual, 0, 'All salary values are valid');
                    ELSE
                        v_failed_count := v_failed_count + 1;
                        INSERT INTO s_sql_dds.t_dq_check_results 
                        (check_type, table_name, column_name, check_name, status, actual_value, error_threshold, error_message)
                        VALUES ('validity', 't_dm_task', 'salary', 'Salary range validation', 'failed', 
                                v_actual, 0, 'Found invalid salary values');
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    v_failed_count := v_failed_count + 1;
                    INSERT INTO s_sql_dds.t_dq_check_results 
                    (check_type, table_name, check_name, status, error_message)
                    VALUES ('validity', 't_dm_task', 'Salary validation', 'error', 
                            'Error: ' || SQLERRM);
                END;
                
                -- Итоговая статистика
                INSERT INTO s_sql_dds.t_dq_check_results 
                (check_type, table_name, check_name, status, expected_value, actual_value, error_message)
                VALUES ('summary', 't_dm_task', 'Overall DQ check', 
                        CASE WHEN v_failed_count = 0 THEN 'passed' ELSE 'failed' END,
                        v_check_count, v_passed_count,
                        CONCAT('Total: ', v_check_count, ', Passed: ', v_passed_count, ', Failed: ', v_failed_count));
                
            END;
            $$ LANGUAGE plpgsql;
        """)
        
        print("Все таблицы и функции успешно созданы!")
        print("Data Quality компоненты добавлены!")
        
    except Exception as e:
        print(f"Ошибка при инициализации базы данных: {e}")
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    init_database()