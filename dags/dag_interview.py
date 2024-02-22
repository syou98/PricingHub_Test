from airflow import DAG
from datetime import datetime
from utils.tasks_utils import get_task_operator

default_args = {
    'owner': 'Youness S.'
}

with DAG(
        dag_id='dag_interview',
        default_args=default_args,
        start_date=datetime(2024, 2, 20),
        description="DAG implementation for PricingHub interview",
        schedule_interval=None
        ) as dag:
    task_1 = get_task_operator(dag=dag, task_number=1)
    task_2 = get_task_operator(dag=dag, task_number=2)
    task_3 = get_task_operator(dag=dag, task_number=3)

    task_1 >> task_2 >> task_3
