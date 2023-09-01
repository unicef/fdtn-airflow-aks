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
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.common import exceptions
from PIL import Image as PILImage
import sqlalchemy
from sqlalchemy import create_engine
import h3
import datetime
from datetime import date, datetime, timedelta
from dateutil import parser
from selenium.webdriver import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import random 
from random import randint 
import getpass


loginURL='https://www.facebook.com'
loginName=${{ secrets.META_LOGIN }}
