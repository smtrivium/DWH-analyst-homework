from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import logging
import socket
from urllib.parse import urlparse
from airflow.exceptions import AirflowException

# Настройка логирования
logger = logging.getLogger(__name__)
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'tags': ['yandex_cloud', 'iss_tracker', 'analytics'],
}

def get_connection_params(hook):
    """Получаем параметры подключения из hook"""
    conn = hook.get_connection(hook.postgres_conn_id)
    return {
        'host': conn.host,
        'port': conn.port,
        'dbname': conn.schema,
        'user': conn.login,
        'password': conn.password,
        'sslmode': conn.extra_dejson.get('sslmode', 'prefer'),
        'sslrootcert': conn.extra_dejson.get('sslrootcert', '')
    }

def test_postgres_connection(**context):
    """Проверка подключения к PostgreSQL с расширенной диагностикой"""
    results = {}
    hook = PostgresHook(postgres_conn_id='analytics_postgres')
    conn_params = get_connection_params(hook)
    
    logger.info(f"Testing connection to {conn_params['user']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}")
    
    try:
        # 1. Проверка DNS
        ip = socket.gethostbyname(conn_params['host'])
        results['dns'] = f"Success: {conn_params['host']} → {ip}"
        logger.info(results['dns'])
        
        # 2. Проверка порта
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        result = sock.connect_ex((conn_params['host'], conn_params['port']))
        sock.close()
        if result == 0:
            results['port'] = f"Success: Port {conn_params['port']} is open"
            logger.info(results['port'])
        else:
            raise AirflowException(f"Port {conn_params['port']} is closed (error {result})")
        
        # 3. Проверка подключения к PostgreSQL
        conn = hook.get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            pg_version = cur.fetchone()[0]
            results['connection'] = f"Success: PostgreSQL {pg_version}"
            logger.info(results['connection'])
            
            # Проверка SSL
            cur.execute("SHOW ssl")
            ssl_status = cur.fetchone()[0]
            results['ssl'] = f"SSL: {'enabled' if ssl_status == 'on' else 'disabled'}"
            logger.info(results['ssl'])
            
            # Проверка существования таблицы
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'iss_positions'
                )
            """)
            table_exists = cur.fetchone()[0]
            results['table_check'] = f"Table exists: {table_exists}"
            logger.info(results['table_check'])
            
            if not table_exists:
                logger.info("Creating iss_positions table")
                cur.execute("""
                    CREATE TABLE iss_positions (
                        id SERIAL PRIMARY KEY,
                        timestamp BIGINT,
                        latitude FLOAT,
                        longitude FLOAT,
                        created_at TIMESTAMP,
                        status VARCHAR(20),
                        error_message TEXT
                    )
                """)
                conn.commit()
                results['table_creation'] = "Table created successfully"
                logger.info(results['table_creation'])
        
        # Сохраняем результаты проверки в XCom
        context['ti'].xcom_push(key='connection_test_results', value=results)
        return results
        
    except Exception as e:
        logger.error("Connection test failed", exc_info=True)
        raise AirflowException(f"PostgreSQL test failed: {str(e)}")

def fetch_iss_data(**context):
    """Получаем текущие координаты МКС с API open-notify.org"""
    url = "http://api.open-notify.org/iss-now.json"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Валидация данных
        if 'iss_position' not in data or 'timestamp' not in data:
            raise ValueError("Invalid API response format")
            
        result = {
            'timestamp': data['timestamp'],
            'latitude': float(data['iss_position']['latitude']),
            'longitude': float(data['iss_position']['longitude']),
            'created_at': datetime.utcnow().isoformat(),
            'status': 'success',
            'error_message': None
        }
        
        # Сохраняем данные в XCom для следующей задачи
        context['ti'].xcom_push(key='iss_data', value=result)
        return result
        
    except requests.exceptions.RequestException as e:
        error_msg = f"API request failed: {str(e)}"
        logger.error(error_msg)
        result = {
            'timestamp': int(datetime.utcnow().timestamp()),
            'latitude': None,
            'longitude': None,
            'created_at': datetime.utcnow().isoformat(),
            'status': 'failed',
            'error_message': error_msg
        }
        context['ti'].xcom_push(key='iss_data', value=result)
        return result
    except (ValueError, KeyError) as e:
        error_msg = f"Data validation error: {str(e)}"
        logger.error(error_msg)
        result = {
            'timestamp': int(datetime.utcnow().timestamp()),
            'latitude': None,
            'longitude': None,
            'created_at': datetime.utcnow().isoformat(),
            'status': 'failed',
            'error_message': error_msg
        }
        context['ti'].xcom_push(key='iss_data', value=result)
        return result

def save_to_postgres(**context):
    """Сохраняет данные о положении МКС в PostgreSQL"""
    # Получаем данные из предыдущей задачи
    iss_data = context['ti'].xcom_pull(task_ids='fetch_iss_data', key='iss_data')
    
    if not iss_data:
        raise AirflowException("No ISS data received from previous task")
    
    # Подготовка SQL запроса
    sql = """
    INSERT INTO iss_positions (timestamp, latitude, longitude, created_at, status, error_message)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    params = (
        iss_data['timestamp'],
        iss_data['latitude'],
        iss_data['longitude'],
        iss_data['created_at'],
        iss_data['status'],
        iss_data['error_message']
    )
    
    try:
        # Используем PostgresHook для подключения
        hook = PostgresHook(postgres_conn_id='analytics_postgres')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Выполняем запрос
        cursor.execute(sql, params)
        conn.commit()
        
        logger.info(f"Successfully saved ISS data to PostgreSQL: {iss_data}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to save data to PostgreSQL: {str(e)}", exc_info=True)
        raise AirflowException(f"Database operation failed: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

with DAG(
    'yandex_cloud_iss_tracker_enhanced',
    default_args=default_args,
    description='Fetch ISS position and save to Yandex Cloud PostgreSQL with connection testing',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    max_active_runs=1
) as dag:
    
    test_connection = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_connection,
        provide_context=True,
    )
    
    fetch_data = PythonOperator(
        task_id='fetch_iss_data',
        python_callable=fetch_iss_data,
        provide_context=True,
    )
    
    save_data = PythonOperator(
        task_id='save_to_postgres',
        python_callable=save_to_postgres,
        provide_context=True,
    )
    
    test_connection >> fetch_data >> save_data