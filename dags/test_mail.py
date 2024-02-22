

import airflow 
from datetime import timedelta 
from airflow import DAG 
from datetime import datetime, timedelta , date
import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import json

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
  hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

  # get the recent disasters
  df_recent_disasters = hook.get_pandas_df(sql="select event_id,  htmldescription, fromdate from public.disasters ;")

  date_filter = date.today() - timedelta(days=30)
  df_recent_disasters=df_recent_disasters[df_recent_disasters["fromdate"]>= date_filter]


 # keep only the disasters that tick some criterias: Orange disasters or disasters impacting Myanmar or PNG 
  df_recent_disasters=df_recent_disasters[df_recent_disasters['htmldescription'].str.contains("Red|Orange|Papua|Myanmar|papua|myanmar|red|orange")]


  # get the disasters already sent via mail 
  df_already_sent = hook.get_pandas_df(sql="select event_id from public.emergency_update_mail group by 1 ;")

  # Remove the disasters that have already had an email sent 
  df_recent_disasters= df_recent_disasters[~ df_recent_disasters['event_id'].isin(list(df_already_sent['event_id']))] 




  # send an email for each disaster in the list  
  if len(df_recent_disasters)>0:   
    for event_id in df_recent_disasters['event_id']:

      # get the impacted popualtion figures 


      sql_query_pop= f"""SELECT  sum(general) as general
      , sum(children_0_19) as children_0_19
      , sum(children_under_1) as children_under_1
      , sum(children_0_4) as children_0_4
      , sum(children_5_14) as children_5_14
      , sum(children_14_19) as children_14_19
      , sum(boys_0_19) as boys_0_19
      , sum(girls_0_19) as girls_0_19
      , sum(women) as women
      from public.population_by_region_wp WHERE event_id = {event_id} 
      """


      population_disaster = hook.get_pandas_df(sql=sql_query_pop )


      sql_query_name = f"""SELECT htmldescription from public.disasters WHERE event_id = {event_id}  """

      name_disaster = hook.get_pandas_df(sql=sql_query_name )

      general= format(population_disaster ['general'][0])
      children_0_19= format(population_disaster ['children_0_19'][0])
      children_under_1= format(population_disaster ['children_under_1'][0])
      children_0_4= format(population_disaster ['children_0_4'][0])
      children_under_5= format(population_disaster ['children_under_1'][0]  + population_disaster ['children_0_4'][0])
      children_5_14= format(population_disaster ['children_5_14'][0])
      children_14_19= format(population_disaster ['children_14_19'][0])
      boys_0_19= format(population_disaster ['boys_0_19'][0] )
      girls_0_19= format(population_disaster ['girls_0_19'][0] )
      women= format(population_disaster ['women'][0] )
      name_disaster_str=name_disaster['htmldescription'][0]

      limited_disaster_name=name_disaster_str[:name_disaster_str.find("at:")]



      # define the html string to be used in the mail

      html_str_mail= f"""
          <div id="intro" class="float-left" style="font-size: 16px" > Hello, <br> 
          We have just identified a recent natural disaster in our region.<br>
          You'll find below a quick summary of the impacted area and population.<br> 

          </div>

          <div id="misc-title" class="float-left" style="font-size: 30px; color: #1CABE2 ; padding: 10px" > {name_disaster_str} </div>

          <div id="sitrep" style="display:flex">

          <img src="https://sitrepapistaging.azurewebsites.net/disaster/{event_id}/image.png" alt="img" />

           <div id="main-numba"
              style="max-width: 200px; padding: 0px 30px; width: 80%;">
              
             <div class="additional-wrapper"
              style="width: 150px; 
              height: 165px; 
              padding: 0px 30px;
              background-color: #edf0f0;  
              
              text-align:center;
              margin-bottom: 20px;">
                                      
               <b class="align-center format-this-please" style=" font-size: 40px; color: #1CABE2; padding-top: 110px ;vertical-align: middle;"> {general}</b>
               <span class="align-center" style="font-size: 20px; text-align:center; padding-top: 100px ;vertical-align: middle; ">Estimated Total Population </span>

             </div>

             <div class="additional-wrapper"
              style="width: 150px; 
              height: 165px; 
              padding: 0px 30px;
              background-color: #edf0f0;  
              text-align:center;
              margin-bottom: 0px; ">
                                      
               <b class="align-center format-this-please" style=" font-size: 40px; color: #1CABE2;padding-top: 110px ;vertical-align: middle;"> {children_0_19}</b>
               <span class="align-center" style="font-size: 20px; text-align:center; padding-top: 100px ;vertical-align: middle; ">Estimated Children 0-19 </span>

           
             </div>

           </div>


          <div id="misc-additional-numba"
              style="max-width: 300px; padding: 0px 0px; width: 80%;">
              
             <div class="additional-wrapper"
              style="width: 320px; 
              height: 40px; 
              padding: 0px 20px;
              background-color: #edf0f0;  
              align-items: center;
              margin-bottom: 12px; ">
                                      
               <b class="align-center format-this-please" style=" font-size: 25px; padding-right: 10px ; color: #1CABE2;padding-top: 25px ;vertical-align: middle; "> {children_under_1}</b>
               <span class="align-center" style="font-size: 18px;padding-top: 25px ;vertical-align: middle;">Estimated Children Under 1 </span>

             </div>

             <div class="additional-wrapper"
              style="width: 320px; 
              height: 40px; 
              padding: 0px 20px;
              background-color: #edf0f0;  
              align-items: center;
              margin-bottom: 12px; ">
                                      
               <b class="align-center format-this-please" style=" font-size: 25px; padding-right: 10px ;color: #1CABE2; padding-top: 25px ;vertical-align: middle; "> {children_under_5}</b>
               <span class="align-center" style="font-size: 18px;padding-top: 25px ;vertical-align: middle;">Estimated Children Under 5 </span>

             </div>

             <div class="additional-wrapper"
              style="width: 320px; 
              height: 40px; 
              padding: 0px 20px;
              background-color: #edf0f0;  
              align-items: center;
              margin-bottom: 12px; ">
                                      
               <b class="align-center format-this-please" style=" font-size: 25px; padding-right: 10px ; color: #1CABE2;padding-top: 25px ;vertical-align: middle; ">{children_5_14}</b>
               <span class="align-center" style="font-size: 18px;padding-top: 25px ;vertical-align: middle;">Estimated Children 5-14 </span>

             </div>

             <div class="additional-wrapper"
              style="width: 320px; 
              height: 40px; 
              padding: 0px 20px;
              background-color: #edf0f0;  
              align-items: center;
              margin-bottom: 11px; ">
                                      
               <b class="align-center format-this-please" style=" font-size: 25px; padding-right: 10px ; color: #1CABE2;padding-top: 25px ;vertical-align: middle; ">{children_14_19}</b>
               <span class="align-center" style="font-size: 18px;padding-top: 25px ;vertical-align: middle;">Estimated Children 15-19 </span>

             </div>

             <div class="additional-wrapper"
              style="width: 320px; 
              height: 40px; 
              padding: 0px 20px;
              background-color: #edf0f0;  
              align-items: center;
              margin-bottom: 11px; ">
                                      
               <b class="align-center format-this-please" style=" font-size: 25px; padding-right: 10px ; color: #1CABE2;padding-top: 25px ;vertical-align: middle; "> {girls_0_19}</b>
               <span class="align-center" style="font-size: 18px;padding-top: 25px ;vertical-align: middle;">Estimated Girls 0-19</span>

             </div>

             <div class="additional-wrapper"
              style="width: 320px; 
              height: 40px; 
              padding: 0px 20px;
              background-color: #edf0f0;  
              align-items: center;
              margin-bottom: 12px; ">
                                      
               <b class="align-center format-this-please" style=" font-size: 25px; padding-right: 10px ; color: #1CABE2;padding-top: 25px ;vertical-align: middle; "> {boys_0_19}</b>
               <span class="align-center" style="font-size: 18px;padding-top: 25px ;vertical-align: middle;">Estimated Boys 0-19</span>

             </div>

             <div class="additional-wrapper"
              style="width: 320px; 
              height: 40px; 
              padding: 0px 20px;
              background-color: #edf0f0;  
              align-items: center;
              margin-bottom: 0px; ">
                                      
               <b class="align-center format-this-please" style=" font-size: 25px; padding-right: 10px ;color: #1CABE2; padding-top: 25px ;vertical-align: middle; "> {women}</b>
               <span class="align-center" style="font-size: 18px;padding-top: 25px ;vertical-align: middle;">Estimated Women </span>

             </div>


           </div>


          </div>

          <div id="link_sitrep" class="float-left" style="font-size: 16px; padding: 10px 0px  10px 0px" > <br> Find out more about this natural disaster here: <a href="https://sitrep-staging.azurewebsites.net/#/sitrep/{event_id}">LINK</a> <br>
          Meta (Facebook) usually provides Connectivity and Population displacement data for the major disasters - if you need that data and it's not visible on the platform, contact us asap so we can request it to Meta <br>
          If you have any additional questions, you can also contact Hugo Ruiz Verastegui - huruiz@unicef.org
          <br><br> Best regards, <br>
          The EAPRO Frontier Data Tech Node
          </div>

          """

      # send the mail 
      subject = f"Natural disaster update - {limited_disaster_name}"
      cc = "huruiz@unicef.org"
      body = html_str_mail
      sender = os.getenv('REQUEST_MAIL_META_FROM')
      recipients = json.loads('["huruiz@unicef.org","huruiz@unicef.org"]')
      password = os.getenv('REQUEST_MAIL_META_APP_PASSWORD')


      msg = MIMEMultipart("alternative")
      body = MIMEText(body , 'html')
      msg.attach(body)        

      msg['Subject'] = subject
      msg['From'] = sender
      msg['To'] = ', '.join(recipients)
      msg['Cc'] = ', '.join(cc)
      with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
         smtp_server.login(sender, password)
         smtp_server.sendmail(sender, recipients, msg.as_string())

   #add the time stamp for the date the mail was sent 
    df_recent_disasters['date_SENT']=date.today()

   # Save the table into a csv to be uploaded into SQL in a second step 
    df_recent_disasters.to_csv('/tmp/update_emergency_mail.csv', index=False)  


with DAG(
    ## MANDATORY 
    dag_id='mail_emergency_test',
    start_date=datetime(2022,11,28),
    default_args=default_args,
    description='sitrep emergency emails',
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
