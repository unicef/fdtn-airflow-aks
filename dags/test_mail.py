

import airflow 
from datetime import timedelta 
from airflow import DAG 
from datetime import datetime, timedelta 
import smtplib
from email.mime.text import MIMEText
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.email_operator import EmailOperator


## Arguments applied to the tasks, not the DAG in itself 
default_args={
    'owner':'airflow',
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay': timedelta(minutes=1)
}

subject = "Test email GDACS"
body = "this is a success"
sender = "unicef.data.eapro@gmail.com"
recipients = ["hugorv54@gmail.com"]
password = "svdh gonx kfch jahb"

def send_email():
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
       smtp_server.login(sender, password)
       smtp_server.sendmail(sender, recipients, msg.as_string())
    print("Message sent!")


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
        send_email = EmailOperator( 
        task_id='send_email', 
        to='huruiz@unicef.org', 
        subject='email test', 
        html_content=" this has been a success" )
        #,dag=dag_email)

        
        send_email_2= PythonOperator(
            task_id="send_email_python",
            python_callable=send_email
            )
    
        send_email
        send_email_2
