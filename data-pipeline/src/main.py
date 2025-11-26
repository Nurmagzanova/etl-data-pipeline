from etl import etl
from fill_dm_table import fill_dm_table
from migrate_to_mysql import migrate_to_mysql
import sys

def main():
    """
    Основная функция запуска всего пайплайна
    """
    skip_mysql = '--skip-mysql' in sys.argv
    
    try:
        print("=== Starting Complete Data Pipeline ===")
        
        # Шаг 1: ETL процесс
        print("\n1. Running ETL process...")
        etl()
        
        # Шаг 2: Заполнение DWH в PostgreSQL
        print("\n2. Loading data to PostgreSQL DWH...")
        fill_dm_table()
        
        # Шаг 3: Миграция в MySQL (пропускаем если указан флаг)
        if not skip_mysql:
            print("\n3. Migrating data to MySQL DWH...")
            migrate_to_mysql()
        else:
            print("\n3. Skipping MySQL migration (--skip-mysql flag)")
        
        print("\n=== Complete Pipeline finished successfully ===")
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()