

import airflow 
from datetime import timedelta 
from airflow import DAG 
from datetime import datetime, timedelta 
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.MIMEImage import MIMEImage

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


    html_code_test_working= f"""
                    <div id="misc-img-wrapper" 
                    style="background-image: url('https://sitrepapistaging.azurewebsites.net/disaster/1000072/image.png');
                        background-repeat: no-repeat; 
                        height: 400px; 
                        width: 400px; 
                        display:block; 
                        margin: 0 auto;
                        ">
                    </div> """


    css_code_test = """
        #sitrep {   background: white     
        #misc-title { font-size: 30px; font-family: "universLTPro-bold"; color: #1CABE2; background: white}      
        }

        
        #misc-main { display: flex; border-bottom: 1px solid $header-border-clr; padding-top: 20px}
            
        #misc-map {
                width: 400px; margin-bottom: 30px;
                color: #1CABE2;
                font-size: 20px; font-family: "universLTPro-bold"}
                
        #misc-img-wrapper {
                    width: 100%; height: 400px;
                    background-size: cover; 
                    background-repeat: no-repeat; 
                    background-position: 50\% 50\%; }
        
        #misc-numba {flex: 1; margin-left: 80px; font-size: 20px;}

        #misc-numba-wrapper {
                    display: flex;
                    span { font-size: 17px; }
                    b { color: #1CABE2; } 
                    
                    }
        
        #misc-numba-additional {
            max-width: 400px; padding: 0px 40px; width: 80%;
            .additional-wrapper {
                display: flex; background: #edf0f0; max-width:600px ; height: 45px; padding: 0px 50px; width: 100%; align-items: center;
                &:not(:last-child) { margin-bottom: 14px; }
                b { flex: 2; font-size: 30px; padding-right: 10px; }
                span { flex: 3;font-size: 20px; }
            }
        }
         """
    with open('summary_img_test.css', 'w+') as f_out:
        f_out.write(css_code_test)

    imgkit.from_string(html_code_test_working, 'out.jpg', css='summary_img_test.css')

    # This example assumes the image is in the current directory
    fp = open('out.jpg', 'rb')
    msgImage = MIMEImage(fp.read())
    fp.close()
    
    
    subject = "Test email"
    body = "This is a test "
    sender = os.getenv('REQUEST_MAIL_META_FROM')
    recipients = json.loads(os.getenv('REQUEST_MAIL_META_TO'))
    password = os.getenv('REQUEST_MAIL_META_APP_PASSWORD')

    
    msg = MIMEMultipart()
    body = MIMEText(body , 'html')
    msg.attach(body)        

    # Define the image's ID as referenced above
    msg.attach(msgImage)
    
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
       smtp_server.login(sender, password)
       smtp_server.sendmail(sender, recipients, msg.as_string())
    print("Message sent!")

    
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
            task_id="send_email_python",
            python_callable=send_email_function
            )

        send_email
