import psycopg2
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'data-pipeline', 'src'))

try:
    from config import DB_CONFIG
except ImportError:
    DB_CONFIG = {
        'host': 'localhost',
        'port': '5432',
        'database': 'etl_db',
        'user': 'user',
        'password': 'password'
    }

def fix_function_completely():
    """
    Полностью пересоздаем функцию без ошибок
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()
        
        print("🔧 Полное исправление функции fn_dm_data_load...")
        
        # Полностью удаляем и пересоздаем функцию
        cur.execute("""
            DROP FUNCTION IF EXISTS s_sql_dds.fn_dm_data_load(DATE, DATE);
            
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
                WHERE (start_dt IS NULL OR effective_from >= start_dt)
                  AND (end_dt IS NULL OR effective_to <= end_dt)
                ON CONFLICT (customer_name) DO NOTHING;

                -- Вставка данных в справочник продуктов
                INSERT INTO s_sql_dds.t_dim_product (product_category)
                SELECT DISTINCT product_category 
                FROM s_sql_dds.t_sql_source_structured
                WHERE (start_dt IS NULL OR effective_from >= start_dt)
                  AND (end_dt IS NULL OR effective_to <= end_dt)
                ON CONFLICT (product_category) DO NOTHING;

                -- Вставка данных в справочник регионов
                INSERT INTO s_sql_dds.t_dim_region (region_name)
                SELECT DISTINCT region 
                FROM s_sql_dds.t_sql_source_structured
                WHERE (start_dt IS NULL OR effective_from >= start_dt)
                  AND (end_dt IS NULL OR effective_to <= end_dt)
                ON CONFLICT (region_name) DO NOTHING;

                -- Вставка данных в справочник статусов
                INSERT INTO s_sql_dds.t_dim_status (status_name)
                SELECT DISTINCT customer_status 
                FROM s_sql_dds.t_sql_source_structured
                WHERE (start_dt IS NULL OR effective_from >= start_dt)
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
        
        print("✅ Функция полностью пересоздана!")
        
        # Тестируем функцию
        print("🧪 Тестируем функцию...")
        cur.execute("SELECT s_sql_dds.fn_dm_data_load(NULL, NULL)")
        print("✅ Функция работает корректно!")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    fix_function_completely()