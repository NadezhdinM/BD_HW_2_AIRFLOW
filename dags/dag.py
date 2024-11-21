import os
import pandas as pd
import subprocess
import logging
from io import StringIO
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import TaskInstance

# Функция для загрузки данных из файла profit_table.csv
def load_data_from_csv(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        logging.error(f"File {file_path} does not exist.")
        raise FileNotFoundError(f"File {file_path} not found.")
    df = pd.read_csv(file_path)
    return df

# Функция для передачи данных во внешний скрипт через stdin и получения результата
def run_transform_script(product: str, df: pd.DataFrame, ti: TaskInstance) -> None:
    product_data = df[['id', 'date', f'sum_{product}', f'count_{product}']]
    csv_data = product_data.to_csv(index=False)

    try:
        result = subprocess.run(
            ['python3.9', '/opt/airflow/your_project/scripts/transform_script.py', product],
            input=csv_data,
            text=True,
            capture_output=True,
            check=True
        )
        ti.xcom_push(key=f'product_{product}_result', value=result.stdout)
        logging.info(f"Product {product} processed and result pushed to XCom.")
    
    except subprocess.CalledProcessError as e:
        logging.error(f"Error while processing product {product}: {e.stderr}")
        ti.xcom_push(key=f'product_{product}_result', value=None)
        
    return result.stdout

# Функция для объединения результатов всех задач
def combine_results_and_save(ti: TaskInstance) -> None:
    main_df = None
    for product in ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']:
        task_id = f'product_processing.transform_{product}'
        result = ti.xcom_pull(task_ids=task_id, key=f'product_{product}_result')

        if result:
            product_df = pd.read_csv(StringIO(result))
            flag_columns = [col for col in product_df.columns if 'flag' in col]

            if product == 'a':
                main_df = product_df
            else:
                if flag_columns:
                    main_df[f'product_{product}_flag'] = product_df[flag_columns[0]]

        else:
            logging.warning(f"No result found for product: {product}")
    
    try:
        file_path = '/opt/airflow/your_project/flags_activity.csv'
        if os.path.exists(file_path):
            main_df.to_csv(file_path, mode='a', header=False, index=False)
        else:
            main_df.to_csv(file_path, mode='w', header=True, index=False)
        logging.info("Combined data appended to flags_activity.csv.")
    except Exception as e:
        logging.error(f"Error saving data: {e}")

# Основная DAG функция
def create_dag(dag_name: str, default_args: dict, schedule_interval: str) -> DAG:
    with DAG(dag_name, default_args=default_args, schedule_interval=schedule_interval, catchup=False) as dag:
        input_file = '/opt/airflow/your_project/data/profit_table.csv'

        df = load_data_from_csv(input_file)

        with TaskGroup("product_processing", dag=dag) as group:
            transform_tasks = []
            for product in ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']:
                transform_task = PythonOperator(
                    task_id=f"transform_{product}",
                    python_callable=run_transform_script,
                    op_args=[product, df],
                    dag=dag,
                )
                transform_tasks.append(transform_task)

        combine_task = PythonOperator(
            task_id='combine_results',
            python_callable=combine_results_and_save,
            dag=dag,
        )

        for transform_task in transform_tasks:
            transform_task >> combine_task

        return dag

# Параметры по умолчанию для DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 11, 1),
}

dag = create_dag(
    'product_processing_dag',
    default_args,
    '0 0 5 * *'  # CRON выражение для ежемесячного запуска
)
