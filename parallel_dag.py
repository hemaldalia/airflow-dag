from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from datetime import datetime
# ADD COMMENT
default_args = {
    'start_date': datetime(2020,7, 1),
    'owner': 'Airflow',
}

def process(**kwargs):
    task_params = kwargs['dag_run'].conf['jobId']
    print('DB Job Id {}'.format(task_params))


with DAG(dag_id='parallel_dag', schedule_interval=None, default_args=default_args, catchup=False) as dag:

    task_0 = PythonOperator(task_id='task_0', python_callable=process, provide_context=True)
    # Tasks dynamically generated 
    tasks = [BashOperator(task_id='task_{0}'.format(t), bash_command='sleep 30'.format(t)) for t in range(1, 4)]

    task_5 = BashOperator(task_id='task_5', bash_command='sleep 30')

    task_6 = BashOperator(task_id='task_6', bash_command='echo "pipeline done"')

    task_0 >> tasks >> task_5 >> task_6

