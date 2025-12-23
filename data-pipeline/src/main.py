from etl import etl
from fill_dm_table import fill_dm_table
from migrate_to_mysql import migrate_to_mysql
from run_data_quality_checks import run_data_quality_checks
import sys
import os

def check_database_connection():
    """Быстрая проверка подключения к базе данных"""
    import psycopg2
    from config import DB_CONFIG
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"   ✅ Подключение к БД успешно: {result[0]}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"   ❌ Ошибка подключения: {e}")
        raise

def run_critical_tests():
    """Запуск только критических тестов"""
    import subprocess
    import sys
    
    # Запускаем только самые важные тесты
    test_cases = [
        "test_database_connection",
        "test_tables_exist"
    ]
    
    for test in test_cases:
        print(f"   Запуск теста: {test}")
        result = subprocess.run(
            [sys.executable, "-m", "pytest", 
             f"tests/test_etl.py::TestETLPipeline::{test}", 
             "-v", "--tb=short"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(f"   ✅ Тест {test} пройден")
        else:
            print(f"   ❌ Тест {test} не пройден")
            print(result.stdout)
            raise Exception(f"Тест {test} не пройден")

def main():
    """
    Основная функция запуска всего пайплайна
    """
    skip_mysql = '--skip-mysql' in sys.argv
    fast_mode = '--fast' in sys.argv or os.getenv('CI_FAST_MODE')
    
    try:
        print("=== Запуск пайплайна данных ===")
        
        if fast_mode:
            print("⚡ БЫСТРЫЙ РЕЖИМ АКТИВИРОВАН")
            # Только критичные проверки
            print("\n1. Быстрая проверка подключения...")
            check_database_connection()
            
            print("\n2. Запуск ключевых тестов...")
            run_critical_tests()
            
            print("\n✅ Быстрая проверка завершена (3-5 минут)")
            
        else:
            # Полный пайплайн (как сейчас)
            print("\n1. Выполнение ETL процесса...")
            etl()
            
            print("\n2. Загрузка данных в PostgreSQL DWH...")
            fill_dm_table()
            
            if not skip_mysql:
                print("\n3. Миграция данных в MySQL DWH...")
                migrate_to_mysql()
            else:
                print("\n3. Пропуск миграции в MySQL...")
            
            print("\n4. Запуск проверок качества данных...")
            run_data_quality_checks()
            
            print("\n✅ Полный пайплайн завершен")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        raise

if __name__ == "__main__":
    main()