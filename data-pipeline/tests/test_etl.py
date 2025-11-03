import pytest
import psycopg2
from src.config import DB_CONFIG
from src.fill_structured_table import fill_structured_table_test

class TestETL:
    
    def test_database_connection(self):
        """Тест подключения к базе данных"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            result = cur.fetchone()
            cur.close()
            conn.close()
            assert result[0] == 1
        except Exception as e:
            pytest.fail(f"Database connection failed: {e}")
    
    def test_etl_function(self):
        """Тест ETL функции"""
        try:
            processed_count = fill_structured_table_test('2023-01-01', '2023-12-31')
            assert processed_count >= 0
            print(f"Тестовая ETL обработала {processed_count} записей")
        except Exception as e:
            pytest.fail(f"ETL function test failed: {e}")
    
    def test_table_exists(self):
        """Тест существования таблиц"""
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()
            
            # Проверка существования таблиц
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 's_sql_dds' 
                AND table_name IN ('t_sql_source_unstructured', 't_sql_source_structured');
            """)
            tables = [row[0] for row in cur.fetchall()]
            
            cur.close()
            conn.close()
            
            assert 't_sql_source_unstructured' in tables
            assert 't_sql_source_structured' in tables
            
        except Exception as e:
            pytest.fail(f"Table existence test failed: {e}")

if __name__ == "__main__":
    pytest.main([__file__])