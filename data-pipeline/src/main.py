from etl import etl
from fill_dm_table import fill_dm_table
from migrate_to_mysql import migrate_to_mysql
from run_data_quality_checks import run_data_quality_checks
import sys

def main():
    """
    Основная функция запуска всего пайплайна
    """
    skip_mysql = '--skip-mysql' in sys.argv
    
    try:
        print("=== Запуск полного пайплайна данных ===")
        
        # Шаг 1: ETL процесс
        print("\n1. Выполнение ETL процесса...")
        etl()
        
        # Шаг 2: Заполнение DWH в PostgreSQL
        print("\n2. Загрузка данных в PostgreSQL DWH...")
        fill_dm_table()
        
        # Шаг 3: Миграция в MySQL (пропускаем если указан флаг)
        if not skip_mysql:
            print("\n3. Миграция данных в MySQL DWH...")
            migrate_to_mysql()
        else:
            print("\n3. Пропуск миграции в MySQL (--skip-mysql флаг)")
        
        # Шаг 4: Проверка качества данных
        print("\n4. Запуск проверок качества данных...")
        run_data_quality_checks()
        
        print("\n=== Полный пайплайн успешно завершен ===")
        
    except Exception as e:
        print(f"Ошибка в пайплайне: {e}")
        raise

if __name__ == "__main__":
    main()