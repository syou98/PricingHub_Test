import unittest

from airflow.exceptions import AirflowSkipException

from test.utils import get_dag, get_context_mocks, TASK_1, TASK_2, TASK_3, get_tasks, get_tasks_and_contexts
from utils.tasks_utils import TASK_NOT_IN_INPUT_MESSAGE, TASKS_FIELD_NOT_A_LIST_MESSAGE, MISSING_TASK_FIELD_MESSAGE


class TestTask(unittest.TestCase):
    def assert_task_skipped_with_message(self, task, context_mock, expected_message):
        with self.assertRaises(AirflowSkipException) as context:
            task.execute(context_mock)
        self.assertIn(expected_message, context.exception.args[0])

    def test_task_with_all_tasks_specified_in_config(self):
        dag = get_dag()
        tasks_and_contexts = get_tasks_and_contexts(dag=dag, conf={"tasks": [TASK_1, TASK_2, TASK_3]})
        for task, context_mock in tasks_and_contexts:
            task.execute(context_mock)

    def test_task_with_task_1_missing_in_config(self):
        dag = get_dag()
        context_mock_task_1, context_mock_task_2, context_mock_task_3 = get_context_mocks(conf={"tasks": [TASK_2, TASK_3]})
        task_1, task_2, task_3 = get_tasks(dag)
        self.assert_task_skipped_with_message(task_1, context_mock_task_1, TASK_NOT_IN_INPUT_MESSAGE)
        task_2.execute(context_mock_task_2)
        task_3.execute(context_mock_task_3)

    def test_task_with_empty_tasks_in_config(self):
        dag = get_dag()
        tasks_and_contexts = get_tasks_and_contexts(dag=dag, conf={"tasks": []})
        for task, context_mock in tasks_and_contexts:
            self.assert_task_skipped_with_message(task, context_mock, TASK_NOT_IN_INPUT_MESSAGE)

    def test_task_with_missing_tasks_field_in_config(self):
        dag = get_dag()
        tasks_and_contexts = get_tasks_and_contexts(dag=dag, conf={})
        for task, context_mock in tasks_and_contexts:
            self.assert_task_skipped_with_message(task, context_mock, MISSING_TASK_FIELD_MESSAGE)

    def test_task_with_non_list_tasks_field_in_config(self):
        dag = get_dag()
        tasks_and_contexts = get_tasks_and_contexts(dag=dag, conf={"tasks": "toto"})
        for task, context_mock in tasks_and_contexts:
            self.assert_task_skipped_with_message(task, context_mock, TASKS_FIELD_NOT_A_LIST_MESSAGE)
