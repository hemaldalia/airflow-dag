import random
import string
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago


dag = DAG(
    dag_id="trigger_job_dag",
    default_args={"owner": "airflow", "start_date": days_ago(1),'provide_context':True},
    catchup=False,
    schedule_interval='*/5 * * * *'    
)


jobid = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(8)])

def tri(context,dag_run_obj):
    dag_run_obj.payload = {"jobId": "ABCDEDEF"}
    return dag_run_obj

trigger = TriggerDagRunOperator(
    task_id='scikit_learn_{0}'.format(jobid),
    trigger_dag_id="scikit_learn_dag",  # Ensure this equals the dag_id of the DAG to trigger
    python_callable=tri,
    dag=dag,
)


