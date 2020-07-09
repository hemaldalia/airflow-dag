from time import sleep
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id="pytorch_dag",
    default_args={"start_date": days_ago(2), "owner": "airflow",'provide_context': True},
    schedule_interval=None
)


def run_this_func(**kwargs):
    print(kwargs["dag_run"].conf["jobId"])
    sleep(30)
    print("Remotely received value of pytorch jobId {}".format(kwargs["dag_run"].conf["jobId"]))
    sleep(15)


run_this = PythonOperator(task_id="run_this", python_callable=run_this_func, dag=dag)

bash_task = BashOperator(
    task_id="bash_task",
    bash_command='sleep 60 && echo "Here is the pytorch jobId: $jobId" && sleep 30',
    env={'jobId': '{{ dag_run.conf["jobId"] if dag_run else "" }}'},
    dag=dag,
)
