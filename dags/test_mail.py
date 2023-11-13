

import airflow 
from datetime import timedelta 
from airflow import DAG 
from datetime import datetime, timedelta 
import smtplib
from email.mime.text import MIMEText
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.email_operator import EmailOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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

subject = "Test email GDACS"
cc = ['huruiz@unicef.org']
body = "Dear Anthony, \
I hope you are doing great and that Vientiane's croissants are exquisite \
We just identified some new high intensity disaster in the East Asia Pacific Region and we would like to start the generation of the Population/ Movements/ Connectivity datasets for the following disaster(s): "
sender = "unicef.data.eapro@gmail.com"
recipients = ["huruiz@unicef.org"]
password = "svdh gonx kfch jahb"

def send_email_function():

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df = hook.get_pandas_df(sql="select event_id, name from public.meta_requests group by 1,2 ;")

    df_html = """\
    <html>
      <head></head>
      <body>
        {0}
      </body>
    </html>
    """.format(df.to_html())
    
    print('print sql results: ')
    print(df)

    msg = MIMEMultipart()
    #msg = MIMEText(body)
    table = MIMEText(df_html, 'html')
    msg.attach(table)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg['Cc'] = ', '.join(cc)
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
        
        send_email= PythonOperator(
            task_id="send_email_python",
            python_callable=send_email_function
            )

        send_email
