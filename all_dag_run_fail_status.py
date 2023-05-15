
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime,timedelta
from pytz import timezone
from airflow import DAG
import requests
from airflow.operators.python import PythonOperator

local_tz = timezone('Asia/Kolkata') 

#
import os
from dotenv import load_dotenv
load_dotenv()

# os.getenv('From_mail')
# os.getenv('Mail_Password')
# os.getenv('To_mail')
# os.getenv('Airflow_Username')
# os.getenv('Airflow_Password')



def get_dag_run_info(dag_id,state,order_by,limit=100):
    dag_run_resp=requests.get('http://localhost:8080/api/v1/dags/{}/dagRuns?limit={}&state={}&order_by={}'.format(dag_id,limit,state,order_by),auth=(os.getenv('Airflow_Username'), os.getenv('Airflow_Password')))
    dag_run_output=dag_run_resp.json()
    return dag_run_output


def task_instances(dag_id,dag_run_id,state,limit=100):
    task_inst=requests.get('http://localhost:8080/api/v1/dags/{}/dagRuns/{}/taskInstances?limit={}&state={}'.format(dag_id,dag_run_id,limit,state),auth=(os.getenv('Airflow_Username'), os.getenv('Airflow_Password')))
    task_inst_resp=task_inst.json()
    total_task=task_inst_resp['total_entries']
    print(f"in {dag_id} dag_id the dag_run_id {dag_run_id} has {total_task} running tasks")
    
    return total_task

def check_running_dag(dag_id):
    total_task_instances=0
    all_dag_runs=get_dag_run_info(dag_id,'running','start_date')
    if all_dag_runs["total_entries"]>0:
        stuck_time=all_dag_runs['dag_runs'][0]['start_date']
        for dag_run in all_dag_runs['dag_runs']:
            tasks_dag_run=task_instances(dag_id,dag_run['dag_run_id'],'running')
            total_task_instances+=tasks_dag_run
        if total_task_instances>=5:
            dag_info={'DAG Name':dag_id,'Total Running Instances':total_task_instances,'Stuck Since':stuck_time,'Latest DAG Status':'running'}
            return dag_info
        else:
            return 'no_running'    

    else:
            return 'no_running'    


def check_failling_dag(dag_id):
    failed_dag_run=get_dag_run_info(dag_id,'failed','-start_date',limit=1)
    if failed_dag_run["total_entries"]>0:
        failed_dag_info={'DAG Name':dag_id,'Failed Since':failed_dag_run['dag_runs'][0]['end_date'],'Latest DAG Status':'failed'} 
        return failed_dag_info 
    else:
        return 'no_failed'      
    

def email_body_table(dag_info,email_body):
#         email_body="<table><thead><tr>"
        for key in dag_info[0].keys():
            email_body=email_body+'<th>'+key+'</th>'
        email_body=email_body+'</tr></thead><tbody>'
        for i in dag_info:
            email_body=email_body+'<tr>'
            for j in i:
                email_body=email_body+'<td align=center>'+str(i[j])+'</td>'
            email_body=email_body+'</tr>'
        email_body=email_body+'</tbody></table>' 
        return email_body

                
def send_mail(subject,email_body):
    smtp_server = smtplib.SMTP('smtp.gmail.com', 587)
    smtp_server.starttls()
    smtp_server.login(os.getenv('From_mail'), os.getenv('Mail_Password'))
    
    message = MIMEMultipart()
    
    body = MIMEText(email_body, 'html')
    message.attach(body)

    

    message['From'] = os.getenv('From_mail')

    message['To'] = os.getenv('To_mail')
    message['Subject'] = subject


    smtp_server.sendmail(message['From'], message['To'], message.as_string())

    smtp_server.quit()
        


def all_dag_fail_run_status():
    all_running_dag=[]
    all_failed_dag=[]
    response_dags=requests.get('http://localhost:8080/api/v1/dags?limit=100&only_active=true',auth=(os.getenv('Airflow_Username'), os.getenv('Airflow_Password')))
    output=response_dags.json()
    all_dags=output['dags']

    for dag in all_dags:
        dag_run_info=check_running_dag(dag['dag_id'])
        if dag_run_info=='no_running':
            pass
        else:
            all_running_dag.append(dag_run_info)
        dag_failed_info=check_failling_dag(dag['dag_id'])
        if dag_failed_info=='no_failed':
            pass
        else:
            all_failed_dag.append(dag_failed_info)
    print(all_running_dag)
    print(all_failed_dag)
    if len(all_running_dag)>0:
        running_body=email_body_table(dag_info=all_running_dag,email_body="<table border=2 ><thead><caption>DAG's with Multiple Running Instances</caption><tr>")
        if len(all_failed_dag)>0:
            email_body=email_body_table(dag_info=all_failed_dag,email_body=running_body+"</br></br></br></br></br></br><table border=2 ><thead><caption>DAG's in Error State</caption><tr>")
            print(email_body)
            send_mail("!!DAG's STATUS",email_body=email_body)
        else:
            running_body=running_body+"</br></br></br></br></br></br><h2>Failed Dag's are Not Found!</h2>" 
            send_mail("!!DAG's STATUS",email_body=running_body)
            
    elif len(all_failed_dag)>0:
        failed_body=email_body_table(dag_info=all_failed_dag,email_body="<h2>No Running Dag's are present</h2>"+"</br></br></br></br></br></br><table border=2 ><thead><caption>DAG's in Error State</caption><tr>")
        send_mail("!!DAG's STATUS",email_body=failed_body)
                        
    else:
        print('no Running and Failed dags are founded')                    

default_args = {
    'owner': 'krrish',
    'start_date': local_tz.localize(datetime(2023,5,15)),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
    'catchup': False,

}

with DAG(
    dag_id='DAG_status_run_fail',
    default_args=default_args,
    description="Checking all DAG's status",
    schedule_interval=timedelta(minutes=30),
) as dag:
     task1=PythonOperator(
        task_id='task01',
        python_callable=all_dag_fail_run_status,
        
    )
task1 
