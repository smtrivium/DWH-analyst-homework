from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Базовые параметры DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def check_iss_api():
    """Простая проверка доступности API МКС"""
    api_url = "http://api.open-notify.org/iss-now.json"
    
    try:
        logger.info(f"Пробуем подключиться к {api_url}")
        
        # Делаем запрос с таймаутом 10 секунд
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()  # Проверяем HTTP ошибки
        
        data = response.json()
        logger.info(f"Успешное подключение! Данные: {data}")
        
        # Выводим основные данные в лог
        position = data.get('iss_position', {})
        logger.info(f"Текущие координаты МКС: {position.get('latitude')}°N, {position.get('longitude')}°E")
        
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка подключения: {str(e)}")
        raise
    except ValueError as e:
        logger.error(f"Ошибка разбора JSON: {str(e)}")
        raise

# Создаем DAG
with DAG(
    'simple_iss_api_check',
    default_args=default_args,
    description='Простая проверка подключения к API МКС',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['monitoring', 'iss']
) as dag:
    
    # Единственная задача - проверка API
    test_connection = PythonOperator(
        task_id='check_iss_api_connection',
        python_callable=check_iss_api
    )

    # Можно добавить дополнительные задачи, если нужно
    # test_connection >> next_task