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

from PIL import Image as PILImage
import sqlalchemy
from sqlalchemy import create_engine
import h3
import datetime
from datetime import date, datetime, timedelta
from dateutil import parser
import random 
from random import randint 
import getpass


loginURL='https://www.facebook.com'
loginName=os.getenv('META_LOGIN')
loginPass=os.getenv('META_PASSWORD')

print(loginName)     
    
