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

jobList = []
modelList = ['scikit','pytorch']
val = random.randint(1,6);
if val==5:
    for i in range(1,3):
        job = {}
        jobid = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(8)])
        job['jobId'] = jobid
        job['type'] = random.choice(modelList)
        jobList.append(job)


def payload(context, dag_run_obj):
        dag_run_obj.payload = {'jobId' :context['params']['jobId'] }
        return dag_run_obj


def getScikitLearnTask(jobid):
    taskid = 'scikit-learn-'+jobid
    task = TriggerDagRunOperator(task_id=taskid,
                                    trigger_dag_id="scikit_learn_dag",
                                    python_callable=payload,
                                    params={'jobId': jobid},
                                    dag=dag)
    return task

def getPyTorchTask(jobid):
    taskid = 'pytorch-'+jobid
    task = TriggerDagRunOperator(task_id=taskid,
                                    trigger_dag_id="pytorch_dag",
                                    python_callable=payload,
                                    params={'jobId': jobid},
                                    dag=dag)
    return task


start = DummyOperator(
    task_id='start',
    dag=dag
)

tasks = [];

for j in jobList:
    to_run = False
    if j['type']=='scikit':
        jobTask = getScikitLearnTask(j['jobId'])
        to_run = True
    elif j['type']=='pytorch':
        jobTask = getPyTorchTask(j['jobId'])
        to_run = True

    start >> jobTask

