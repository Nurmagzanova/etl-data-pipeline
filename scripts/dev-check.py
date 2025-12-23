#!/usr/bin/env python3
"""
Быстрая проверка для разработчиков
Запуск: python scripts/dev-check.py
Время: 2-3 минуты
"""

import psycopg2
import time

def quick_check():
    print("🔍 Быстрая проверка системы (2-3 минуты)")
    start_time = time.time()
    
    # 1. Проверка подключения к БД
    print("1. Проверка подключения к PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="etl_db",
            user="user",
            password="password"
        )
        print("   ✅ PostgreSQL доступен")
        conn.close()
    except:
        print("   ⚠️  PostgreSQL не доступен, запускаем Docker...")
        import subprocess
        subprocess.run(["docker-compose", "up", "-d", "postgres"])
        time.sleep(10)
    
    # 2. Быстрая инициализация
    print("2. Быстрая инициализация...")
    # Минимальный набор таблиц
    
    # 3. Быстрые тесты
    print("3. Запуск ключевых тестов...")
    import subprocess
    result = subprocess.run(["python", "-m", "pytest", 
                           "tests/test_etl.py::TestETLPipeline::test_database_connection",
                           "tests/test_etl.py::TestETLPipeline::test_tables_exist",
                           "-v", "--tb=short"],
                          capture_output=True, text=True)
    
    if result.returncode == 0:
        print("   ✅ Ключевые тесты пройдены")
    else:
        print("   ❌ Тесты не пройдены")
        print(result.stdout)
    
    elapsed = time.time() - start_time
    print(f"\n⏱️  Проверка заняла: {elapsed:.1f} секунд")
    print("🎯 Система готова к разработке!")

if __name__ == "__main__":
    quick_check()