from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import logging
import socket
from urllib.parse import urlparse
from airflow.exceptions import AirflowException

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0
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

def test_postgres_connection():
    logger = logging.getLogger(__name__)
    results = {}
    
    try:
        hook = PostgresHook(postgres_conn_id='analytics_postgres')
        conn_params = get_connection_params(hook)
        
        logger.info(f"Testing connection to {conn_params['user']}@{conn_params['host']}:{conn_params['port']}/{conn_params['dbname']}")
        
        # 1. Проверка DNS
        try:
            ip = socket.gethostbyname(conn_params['host'])
            results['dns'] = f"Success: {conn_params['host']} → {ip}"
            logger.info(results['dns'])
        except socket.gaierror as e:
            results['dns'] = f"DNS resolution failed: {str(e)}"
            raise AirflowException(results['dns'])
        
        # 2. Проверка порта
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            result = sock.connect_ex((conn_params['host'], conn_params['port']))
            sock.close()
            if result == 0:
                results['port'] = f"Success: Port {conn_params['port']} is open"
                logger.info(results['port'])
            else:
                results['port'] = f"Port {conn_params['port']} is closed (error {result})"
                raise AirflowException(results['port'])
        except Exception as e:
            results['port'] = f"Port check failed: {str(e)}"
            raise AirflowException(results['port'])
        
        # 3. Проверка подключения к PostgreSQL
        try:
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
                
            return True
        except Exception as e:
            results['connection'] = f"Connection failed: {str(e)}"
            raise AirflowException(results['connection'])
            
    except Exception as e:
        logger.error("Connection test failed")
        raise AirflowException(f"PostgreSQL test failed: {str(e)}")
    finally:
        logger.info("\n=== Test Results ===")
        for test, result in results.items():
            logger.info(f"{test.upper():<15} {result}")

with DAG(
    'test_postgres_connection_fixed',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['diagnostic']
) as dag:
    
    test_task = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_connection,
    )