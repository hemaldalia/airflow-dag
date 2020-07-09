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
    schedule_interval='*/1 * * * *'    
)

modelList = ['scikit','pytorch']



def payload(context, dag_run_obj):
        dag_run_obj.payload = {'jobId' :context['params']['jobId'] }
        return dag_run_obj


def getScikitLearnTask(jobid):
    
    return task

def getPyTorchTask(jobid):
    
    return task


start = DummyOperator(
    task_id='start',
    dag=dag
)

tasks = []
jobType = random.choice(modelList)
jobid = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(8)])

if jobType =='scikit':
    jobTask = TriggerDagRunOperator(task_id='scikit_learn_{0}'.format(jobid),
                            trigger_dag_id="scikit_learn_dag",
                            python_callable=payload,
                            params={'jobId': jobid},
                            dag=dag)
elif jobType=='pytorch':
    jobTask = TriggerDagRunOperator(task_id='pytorch_{0}'.format(jobid),
                            trigger_dag_id="pytorch_dag",
                            python_callable=payload,
                            params={'jobId': jobid},
                            dag=dag)

start >> jobTask


