from etl import etl
from fill_dm_table import fill_dm_table
from migrate_to_mysql import migrate_to_mysql
from run_data_quality_checks import run_data_quality_checks
import sys
import os

def quick_check():
    """СУПЕР-БЫСТРАЯ проверка - 30 секунд максимум"""
    print("⚡ СУПЕР-БЫСТРАЯ ПРОВЕРКА (30 секунд)")
    
    # 1. Проверяем только подключение
    try:
        import psycopg2
        from config import DB_CONFIG
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        print("✅ База доступна")
        return True
    except Exception as e:
        print(f"❌ Ошибка БД: {e}")
        return False

def main():
    """
    Основная функция запуска всего пайплайна
    """
    skip_mysql = '--skip-mysql' in sys.argv
    fast_mode = '--fast' in sys.argv or os.getenv('CI_FAST_MODE')
    
    try:
        if fast_mode:
            # ТОЛЬКО проверка подключения - 5 секунд
            print("=== БЫСТРАЯ ПРОВЕРКА ===")
            if quick_check():
                print("✅ Система готова к работе")
            else:
                raise Exception("Быстрая проверка не пройдена")
            return  # ВЫХОДИМ сразу после быстрой проверки!
        
        # Полный пайплайн (только если НЕ fast_mode)
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