from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import date, datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import shutil
import time
import random
import zipfile
import re
import pandas as pd
import glob
from urllib.parse import urlparse



loginURL='https://www.facebook.com'
loginName=os.getenv('META_LOGIN')
loginPass=os.getenv('META_PASSWORD')

print(loginName)     
    
