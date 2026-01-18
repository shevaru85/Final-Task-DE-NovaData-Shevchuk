from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import subprocess
import requests
import os
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    'main',
    default_args=default_args,
    description='Final Task Shevhuk',
    schedule_interval=None,
)

def query_clickhouse(**kwargs):
    response = requests.get('http://clickhouse:8123/?query=SELECT%20version()')
    if response.status_code == 200:
        logging.info(f"ClickHouse version: {response.text}")
    else:
        logging.error(f"Failed to connect to ClickHouse, status code: {response.status_code}")

def query_postgres(**kwargs):
    command = [
        'psql',
        '-h', 'postgres_user',
        '-U', 'user',
        '-d', 'test',
        '-c', 'SELECT version();'
    ]
    env = os.environ.copy()            
    env["PGPASSWORD"] = "password"
    result = subprocess.run(command, env=env, capture_output=True, text=True)
    if result.returncode == 0:
        logging.info(f"PostgreSQL version: {result.stdout}")
    else:
        logging.error(f"Failed to connect to PostgreSQL, error: {result.stderr}")

def pyspark_task(**kwargs):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, mean, expr, regexp_replace, when, floor, isnan
    from pyspark.sql.types import DoubleType, IntegerType
    import pandas as pd
    import os

    data_path = "/opt/airflow/dags/russian_houses.csv"

    if not os.path.exists(data_path):
        raise FileNotFoundError(f"File {data_path} not found")

    # Создаем Spark сессию
    logging.info("Инициализация PySpark сессии...")
    spark = SparkSession.builder \
        .appName("Russian Houses Analysis") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    # Читаем файл сначала через pandas для определения разделителя и корректного чтения UTF-16
    # затем конвертируем в PySpark DataFrame
    logging.info("Чтение CSV файла с кодировкой UTF-16 через pandas...")
    pandas_df = pd.read_csv(data_path, encoding='utf-16')
    
    # Очищаем имена колонок от пробелов
    pandas_df.columns = [col.strip() for col in pandas_df.columns]
    
    # Конвертируем pandas DataFrame в PySpark DataFrame
    logging.info("Конвертация в PySpark DataFrame...")
    df = spark.createDataFrame(pandas_df)
    
    total_rows = df.count()
    logging.info(f"Всего строк в исходном файле: {total_rows}")
    logging.info(f"Колонки: {df.columns}")

    # Проверить формат: отсутствие пустых строк (все поля null/пусто)
    empty_rows = df.filter(
        col('house_id').isNull() & 
        col('latitude').isNull() & 
        col('longitude').isNull()
    ).count()
    print(f"Пустых строк: {empty_rows}")

    # Очистка данных в колонке square: заменяем пробелы и запятые, затем преобразуем в float
    df = df.withColumn('square', regexp_replace(col('square'), ' ', ''))
    df = df.withColumn('square', regexp_replace(col('square'), ',', '.'))
    df = df.withColumn('square', col('square').cast(DoubleType()))
    
    # Преобразуем типы данных
    df = df.withColumn('maintenance_year', col('maintenance_year').cast(IntegerType()))
    df = df.withColumn('population', col('population').cast(IntegerType()))
    df = df.withColumn('latitude', col('latitude').cast(DoubleType()))
    df = df.withColumn('longitude', col('longitude').cast(DoubleType()))
    df = df.withColumn('communal_service_id', col('communal_service_id').cast(DoubleType()))

    # Проверить наличие некорректных годов
    invalid_years = df.filter(
        col('maintenance_year').isNull() | 
        (col('maintenance_year') < 1800) | 
        (col('maintenance_year') > 2025)
    ).count()
    print(f"Некорректных годов обслуживания: {invalid_years}")

    # Средний и медианный год обслуживания
    stats = df.select(
        mean('maintenance_year').alias('avg_year'),
        expr('percentile_approx(maintenance_year, 0.5)').alias('median_year')
    ).collect()[0]
    
    avg_year = stats['avg_year']
    median_year = stats['median_year']
    print(f"Средний год обслуживания: {avg_year:.2f}")
    print(f"Медианный год обслуживания: {median_year}")

    # Топ-10 областей по количеству объектов
    top_regions = df.groupBy('region').count() \
        .orderBy(col('count').desc()) \
        .limit(10)
    print("Топ-10 областей по количеству объектов:")
    top_regions.show(10, truncate=False)

    # Топ-10 населенных пунктов по количеству объектов
    top_localities = df.groupBy('locality_name').count() \
        .orderBy(col('count').desc()) \
        .limit(10)
    print("Топ-10 населенных пунктов по количеству объектов:")
    top_localities.show(10, truncate=False)

    # Здания с максимальной и минимальной площадью в каждой области
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc, asc
    
    # Фильтруем записи с валидной площадью
    df_valid_square = df.filter(col('square').isNotNull())
    
    # Окно для каждой области
    window_max = Window.partitionBy('region').orderBy(desc('square'))
    window_min = Window.partitionBy('region').orderBy(asc('square'))
    
    # Здания с максимальной площадью
    max_area = df_valid_square.withColumn('row_num', row_number().over(window_max)) \
        .filter(col('row_num') == 1) \
        .select('region', 'locality_name', 'square', 'maintenance_year') \
        .orderBy('region')
    
    print("Здания с максимальной площадью в каждой области (первые 20):")
    max_area.show(20, truncate=False)
    
    # Здания с минимальной площадью
    min_area = df_valid_square.withColumn('row_num', row_number().over(window_min)) \
        .filter(col('row_num') == 1) \
        .select('region', 'locality_name', 'square', 'maintenance_year') \
        .orderBy('region')
    
    print("Здания с минимальной площадью в каждой области (первые 20):")
    min_area.show(20, truncate=False)

    # Количество зданий по десятилетиям
    df_with_decade = df.withColumn('decade', (floor(col('maintenance_year') / 10) * 10).cast(IntegerType()))
    decade_stats = df_with_decade.groupBy('decade').count() \
        .orderBy('decade')
    print("Количество зданий по десятилетиям:")
    decade_stats.show(100, truncate=False)
    
    # Сохраняем обработанный DataFrame для последующей загрузки в ClickHouse
    output_dir = "/opt/airflow/dags"
    
    # Удаляем колонку decade (если добавили) для экспорта
    df_export = df.select(
        'house_id', 'latitude', 'longitude', 'maintenance_year', 
        'square', 'population', 'region', 'locality_name', 
        'address', 'full_address', 'communal_service_id', 'description'
    )
    
    logging.info(f"Порядок колонок для экспорта: {df_export.columns}")
    
    # Удаляем старый файл, если существует
    output_file = f"{output_dir}/processed_data.csv"
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"Старый файл {output_file} удален")
    
    # Собираем данные и сохраняем в TSV формат
    # Используем collect() для малых датасетов или toLocalIterator() для больших
    logging.info("Начало экспорта данных в TSV формат...")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        rows_processed = 0
        
        # Используем toLocalIterator для эффективной обработки больших данных
        for row in df_export.toLocalIterator():
            formatted_values = []
            
            for col_name in df_export.columns:
                val = row[col_name]
                
                if val is None:
                    formatted_values.append('\\N')
                elif col_name in ['maintenance_year', 'population']:
                    # Целочисленные значения
                    formatted_values.append(str(int(val)))
                elif col_name in ['latitude', 'longitude', 'square', 'communal_service_id']:
                    # Числа с плавающей точкой
                    formatted_values.append(str(val))
                else:
                    # Строковые значения - экранируем спецсимволы
                    s = str(val)
                    s = s.replace('\\', '\\\\')
                    s = s.replace('\t', '\\t')
                    s = s.replace('\n', '\\n')
                    s = s.replace('\r', '\\r')
                    formatted_values.append(s)
            
            line = '\t'.join(formatted_values)
            f.write(line + '\n')
            
            rows_processed += 1
            
            # Логируем первую строку и прогресс каждые 50000 строк
            if rows_processed == 1:
                print(f"Первая записанная строка (первые 300 символов): {line[:300]}")
            if rows_processed % 50000 == 0:
                print(f"Обработано {rows_processed} строк")
    
    print(f"Данные для загрузки сохранены в {output_file}")
    print(f"Всего обработано строк: {rows_processed}")
    print(f"Размер файла: {os.path.getsize(output_file) / (1024*1024):.2f} MB")
    
    # Проверяем финальное содержимое - первые 3 строки
    print("Проверка первых 3 строк созданного файла:")
    with open(output_file, 'r', encoding='utf-8') as check_f:
        for i in range(3):
            line = check_f.readline()
            print(f"  Строка {i+1}: {line[:300]}")
    
    # Останавливаем Spark сессию
    spark.stop()
    logging.info("PySpark сессия остановлена")

def load_to_clickhouse(**kwargs):
    import requests
    
    # Путь к обработанным данным
    data_path = "/opt/airflow/dags/processed_data.csv"
    
    # Проверяем первые несколько строк файла для отладки
    logging.info("Проверка первых строк файла...")
    with open(data_path, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i < 3:
                logging.info(f"Строка {i}: {line[:300]}")  # Первые 300 символов
            else:
                break
    
    clickhouse_url = 'http://clickhouse:8123/'
    
    # Очищаем таблицу перед загрузкой
    truncate_query = "TRUNCATE TABLE russian_houses"
    response = requests.post(clickhouse_url, data=truncate_query)
    if response.status_code == 200:
        logging.info("Таблица russian_houses очищена")
    else:
        logging.error(f"Ошибка при очистке таблицы: {response.text}")
    
    # Загружаем данные напрямую из файла батчами
    batch_size = 10000
    insert_query = "INSERT INTO russian_houses FORMAT TabSeparated"
    
    with open(data_path, 'r', encoding='utf-8', errors='strict') as f:
        batch_lines = []
        line_count = 0
        
        for line in f:
            # Проверяем, что в строке правильное количество полей (12 полей)
            field_count = line.count('\t') + 1
            if field_count != 12:
                logging.warning(f"Строка {line_count + 1} содержит {field_count} полей вместо 12. Пропускаем.")
                logging.warning(f"Содержимое: {line[:200]}")
                continue
                
            batch_lines.append(line)
            line_count += 1
            
            if len(batch_lines) >= batch_size:
                # Отправляем батч в ClickHouse
                batch_data = ''.join(batch_lines)
                response = requests.post(clickhouse_url, 
                                        params={'query': insert_query}, 
                                        data=batch_data.encode('utf-8'))
                
                if response.status_code == 200:
                    logging.info(f"Загружено строк: {line_count}")
                else:
                    logging.error(f"Ошибка при загрузке батча: {response.text}")
                    # Логируем проблемную строку
                    if batch_lines:
                        logging.error(f"Первая строка батча: {batch_lines[0][:200]}")
                    raise Exception(f"Failed to load data to ClickHouse: {response.text}")
                
                batch_lines = []
        
        # Загружаем последний батч
        if batch_lines:
            batch_data = ''.join(batch_lines)
            response = requests.post(clickhouse_url, 
                                    params={'query': insert_query}, 
                                    data=batch_data.encode('utf-8'))
            
            if response.status_code == 200:
                logging.info(f"Загружено строк: {line_count} (финальный батч)")
            else:
                logging.error(f"Ошибка при загрузке финального батча: {response.text}")
                raise Exception(f"Failed to load data to ClickHouse: {response.text}")
    
    logging.info(f"Успешно загружено {line_count} строк в ClickHouse")

def query_top_houses(**kwargs):
    import requests
    
    # SQL запрос для топ-25 домов с площадью > 60 кв.м
    query = """
    SELECT 
        house_id,
        region,
        locality_name,
        address,
        square,
        maintenance_year,
        population
    FROM russian_houses
    WHERE square > 60
    ORDER BY square DESC
    LIMIT 25
    FORMAT TabSeparatedWithNames
    """
    
    clickhouse_url = 'http://clickhouse:8123/'
    response = requests.post(clickhouse_url, data=query)
    
    if response.status_code == 200:
        logging.info("Топ-25 домов с площадью больше 60 кв.м:")
        logging.info(response.text)
        
        # Сохраняем результат в CSV
        with open("/opt/airflow/dags/top_25_houses.csv", "w", encoding='utf-8') as f:
            f.write(response.text)
        logging.info("Результат сохранен в /opt/airflow/dags/top_25_houses.csv")
    else:
        logging.error(f"Ошибка при выполнении запроса: {response.text}")
        raise Exception(f"Failed to query ClickHouse: {response.text}")

task_query_clickhouse = PythonOperator(
    task_id='query_clickhouse',
    python_callable=query_clickhouse,
    dag=dag,
)

task_query_postgres = PythonOperator(
    task_id='query_postgres',
    python_callable=query_postgres,
    dag=dag,
)

task_pyspark = PythonOperator(
    task_id='pyspark_task',
    python_callable=pyspark_task,
    dag=dag,
)

task_load_clickhouse = PythonOperator(
    task_id='load_to_clickhouse',
    python_callable=load_to_clickhouse,
    dag=dag,
)

task_query_top_houses = PythonOperator(
    task_id='query_top_houses',
    python_callable=query_top_houses,
    dag=dag,
)

task_query_clickhouse >> task_query_postgres >> task_pyspark >> task_load_clickhouse >> task_query_top_houses