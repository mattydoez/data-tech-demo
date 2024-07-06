from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.exceptions import AirflowException
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow import settings

class ExternalTaskWithinDaysSensor(ExternalTaskSensor):
    def __init__(self, days, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.days = days

    def poke(self, context):
        session = settings.Session()
        if self.external_task_id:
            query = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == self.external_dag_id,
                    DagRun.execution_date >= days_ago(self.days),
                    DagRun.state == State.SUCCESS,
                    TaskInstance.task_id == self.external_task_id,
                    TaskInstance.state == State.SUCCESS,
                )
            )
        else:
            query = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id == self.external_dag_id,
                    DagRun.execution_date >= days_ago(self.days),
                    DagRun.state == State.SUCCESS,
                )
            )
        result = query.all()
        session.close()
        if result:
            return True
        else:
            raise AirflowException(f"No successful runs found for {self.external_dag_id} in the last {self.days} days.")