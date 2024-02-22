from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

TASK_NOT_IN_INPUT_MESSAGE = "it is not in triggered tasks"
MISSING_TASK_FIELD_MESSAGE = "input configuration 'tasks' is missing"
TASKS_FIELD_NOT_A_LIST_MESSAGE = "input configuration 'tasks' is not a list"


def raise_skip_exception(task_id: str, error: str = None) -> None:
    """
    Raises an exception to skip a task in the DAG.

    :param task_id: The ID of the task to be skipped.
    :param error: Optional reason for skipping the task. Uses a default message if not provided.
    :return: None.
    """
    exception_message = f"{task_id} skipped : {error if error else TASK_NOT_IN_INPUT_MESSAGE}."
    raise AirflowSkipException(exception_message)


def my_task(**kwargs) -> None:
    """
    Checks if the task is in the 'tasks' list from DAG run configuration and executes it.

    :param kwargs: Contains context like 'task_instance' and 'dag_run'.
    :return: None. May print a message if task in 'tasks' list and raise AirflowSkipException to skip task if not in the 'tasks' list or if 'tasks' is missing or not a list.
    """
    context = kwargs
    task_id = context.get('task_instance').task_id
    try:
        tasks = context["dag_run"].conf["tasks"]
        assert isinstance(tasks, list)
        if task_id in tasks:
            print(f"Hello world from {task_id} !")
        else:
            raise_skip_exception(task_id=task_id)
    except KeyError:
        raise_skip_exception(task_id=task_id, error=MISSING_TASK_FIELD_MESSAGE)
    except AssertionError:
        raise_skip_exception(task_id=task_id, error=TASKS_FIELD_NOT_A_LIST_MESSAGE)


def get_task_operator(dag: DAG, task_number: int) -> PythonOperator:
    """
    Creates a PythonOperator task for an Airflow DAG with predefined "my_task" function as python_callable.

    :param dag: The DAG to which the task is added.
    :param task_number: A unique identifier for the task.
    :return: A PythonOperator to execute "my_task" in the given DAG.
    """
    return PythonOperator(
        task_id=f"task_{task_number}",
        python_callable=my_task,
        provide_context=True,
        trigger_rule="none_failed",
        dag=dag
    )
