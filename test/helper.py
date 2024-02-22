from datetime import datetime
from uuid import uuid4

from airflow import DAG

from utils.tasks_utils import get_task_operator

TASK_1, TASK_2, TASK_3 = "task_1", "task_2", "task_3"


class DagRunMock:
    """
    Mocks an Airflow DAG run for testing.

    :param conf: Configuration for the DAG run mock.
    """
    def __init__(self, conf: dict):
        self.conf = conf


class TaskInstanceMock:
    """
    Mocks an Airflow Task Instance for testing.

    :param task_id: Task ID for the Task mock.
    """
    def __init__(self, task_id: str):
        self.task_id = task_id


def get_dag() -> DAG:
    """
    Generates a test DAG with a unique UUID as its ID.

    The start date is set to the current time.

    :return: A test DAG object.
    """
    return DAG(str(uuid4()), start_date=datetime.now())


def get_context_mocks(conf: dict) -> tuple:
    """
    Creates same configuration based mock contexts for testing with specific task IDs.

    :param conf: Configuration for the mock DAG runs.
    :return: A tuple with three mock contexts for 'task_1', 'task_2', and 'task_3'.
    """
    return {
        "dag_run": DagRunMock(conf=conf),
        "task_instance": TaskInstanceMock(task_id=TASK_1)
    }, {
        "dag_run": DagRunMock(conf=conf),
        "task_instance": TaskInstanceMock(task_id=TASK_2)
    }, {
        "dag_run": DagRunMock(conf=conf),
        "task_instance": TaskInstanceMock(task_id=TASK_3)
    }


def get_tasks(dag: DAG) -> tuple:
    """
    Creates three task operators for a given DAG.
    Uses `get_task_operator` to create tasks.

    :param dag: The DAG to which these tasks will be added.
    :return: A tuple with three PythonOperator tasks for 'task_1', 'task_2', and 'task_3'.
    """
    return get_task_operator(dag=dag, task_number=1), get_task_operator(dag=dag, task_number=2), get_task_operator(dag=dag, task_number=3)


def get_tasks_and_contexts(dag: DAG, conf: dict) -> list:
    """
    Combines tasks and their mock contexts into pairs.

    :param dag: The DAG for which tasks are created.
    :param conf: Configuration for creating mock contexts.
    :return: A list of tuples, each containing a task and its corresponding mock context.
    """
    context_mock_task_1, context_mock_task_2, context_mock_task_3 = get_context_mocks(conf=conf)
    task_1, task_2, task_3 = get_tasks(dag)
    return [
        (task_1, context_mock_task_1),
        (task_2, context_mock_task_2),
        (task_3, context_mock_task_3)
    ]
