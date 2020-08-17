from __future__ import print_function
from pprint import pprint
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


configmaps = ['task-config']

secret_volume = Secret('volume', '/etc/gcp', 'gcs-service-account-credentials', 'sa_credentials.json')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
	'provide_context':True,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}


dag = DAG(
    'kubernetes_pipeline', default_args=default_args, schedule_interval=None)


# [START howto_operator_python]
def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


start = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)
# [END howto_operator_python]

risk_processor = KubernetesPodOperator(
    namespace='default',
    image="eu.gcr.io/starfair/riskprocessor:5.0",
    env_vars={ 'JOB_ID': '{{ dag_run.conf.jobId }}', 'GOOGLE_APPLICATION_CREDENTIALS': '/etc/gcp/sa_credentials.json'},
    labels={"task": "risk-processor"},
    name="risk-processor",
    task_id="risk-processor",
    configmaps=configmaps,
	secrets=[secret_volume],
	image_pull_secrets="gcr-json-key",
    get_logs=True,
	resources={'limit_memory': '200Mi', 'limit_cpu': "0.5"},
    dag=dag
)

end = BashOperator(
    task_id='end',
    bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    dag=dag,
)

start >> risk_processor >> end


