from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.utcnow()
}

dag = DAG(
    'kubernetes_sample', default_args=default_args, schedule_interval=None)



passing = KubernetesPodOperator(namespace='default',
                          image="ubuntu:16.04",
                          cmds=["bash", "-cx"],
                          arguments=["echo", "10"],
                          labels={"foo": "bar"},
                          name="test",
                          task_id="task",
                          dag = dag
                          )


