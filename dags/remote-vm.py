

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import os
import shutil
import time
import random
import zipfile
import re
import pandas as pd
import glob
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.common import exceptions
import sqlalchemy
from sqlalchemy import create_engine
import h3
import datetime
from datetime import date, datetime, timedelta
from dateutil import parser
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import random 
from random import randint 

from azure.identity import DefaultAzureCredential
from azure.mgmt.compute import ComputeManagementClient
import os

## Arguments applied to the tasks, not the DAG in itself 
default_args={
    'owner':'airflow',
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay': timedelta(minutes=1)
}



def test():
  credential = DefaultAzureCredential()
  resource_group = 'LINUX-VM_GROUP'
  vm_name='linux-vm'
  subscription_id = 'cf59aa4a-a61e-4280-9315-5c366a03d507'

  compute_client = ComputeManagementClient(
        credential=credential,
        subscription_id=subscription_id)
  run_command_parameters = {
    'command_id': 'RunShellScript', # For linux, don't change it
    'script': ['ls /tmp']}


  compute_client = ComputeManagementClient(
        credential=credential,
        subscription_id=subscription_id
    )

  poller = compute_client.virtual_machines.begin_run_command(
    resource_group,
    vm_name,
    run_command_parameters);

  result = poller.result()  # Blocking till executed
  print(result.value[0].message)  # stdout/stderr



with DAG(
    ## MANDATORY 
    dag_id='test-remote-vm',
    start_date=datetime(2022,11,28),
    default_args=default_args,
    description='test-remote-vm',
    #schedule not used for the moment as the DAGS run when airflow boots everymorning
    #schedule_interval='0 2 * * *',
    # no need to catch up on the previous runs
    catchup=False
) as dag:

        test = PythonOperator(
            task_id="test",
            python_callable= test
            )

    
