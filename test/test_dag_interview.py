import unittest

from airflow.exceptions import AirflowDagCycleException
from airflow.utils.dag_cycle_tester import check_cycle


class TestDag(unittest.TestCase):
    def test_dag_cycles(self):
        from dags.dag_interview import dag
        try:
            print(dag)
            check_cycle(dag)
        except AirflowDagCycleException:
            assert False, "test failed : the DAG contains a cycle "
