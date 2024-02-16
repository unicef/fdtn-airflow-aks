import airflow 
from datetime import timedelta 
from airflow import DAG 
from datetime import datetime, timedelta 
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

from airflow.operators.python_operator import PythonOperator 
from airflow.operators.email_operator import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import os
from dotenv import load_dotenv
load_dotenv()

#define the connection id to postres
POSTGRES_CONN_ID="postgres_datafordecision"


## Arguments applied to the tasks, not the DAG in itself 
default_args={
    'owner':'airflow',
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay': timedelta(minutes=1)
}



def send_email_function():
    test=os.getenv('REQUEST_MAIL_META_FROM')
    print(os.getenv('REQUEST_MAIL_META_CC'))
    print(os.getenv('REQUEST_MAIL_META_FROM'))
    print(os.getenv('REQUEST_MAIL_META_TO'))
    print(os.getenv('REQUEST_MAIL_META_APP_PASSWORD'))
    
    print("-----")
    print("-----")
    
    print(test)
    return test


with DAG(
    ## MANDATORY 
    dag_id='mail',
    start_date=datetime(2022,11,28),
    default_args=default_args,
    description='sitrep disasters',
    #schedule not used for the moment as the DAGS run when airflow boots everymorning
    #schedule_interval='0 2 * * *',
    # no need to catch up on the previous runs
    catchup=False
) as dag:
        
        send_email= PythonOperator(
            task_id="send_email_summary_python",
            python_callable=send_email_function
            )

        send_email
