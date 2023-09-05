

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



## Arguments applied to the tasks, not the DAG in itself 
default_args={
    'owner':'airflow',
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay': timedelta(minutes=1)
}

#GET the meta user names and login from the env
loginURL='https://www.facebook.com'
loginName=os.getenv('META_LOGIN')
loginPass=os.getenv('META_PASSWORD')

vm_public_ip=os.getenv('VM_PUBLIC_IP')

#list of countries to filter 
country_list_eapr= ["Australia",
                   "Brunei",
                   "Cambodia",
                   "Cook",
                   "Fiji",
                   "Indonesia",
                   "China",
                   "Japan",
                   "Korea",
                   "Kiribati",
                   "Laos",
                   "Malaysia",
                   "Marshall",
                   "Micronesia",
                   "Mongolia",
                   "Myanmar",
                   "Nauru",
                   "Zealand",
                   "Niue",
                   "Palau",
                   "Papua",
                   "Philippines",
                   "Samoa",
                   "Singapore",
                   "Solomon",
                   "Thailand",
                   "Taiwan",
                   "Timor",
                   "Tonga",
                   "Tuvalu",
                   "Vanuatu",
                   "Viet Nam"]

regexp_eapr_str= ('|').join(country_list_eapr)

def is_in_eapr(disaster_name):
    if re.search(regexp_eapr_str.lower().replace(" ", ""), disaster_name.lower().replace(" ", "")):
        return 1
    else :
        return 0

# def useful functions

def random_sleep(factor=1.0):
    sleepTime = (random.random() + 1.0) * factor
    time.sleep(sleepTime)
    return sleepTime

def get_element_contains(element, text):
    return element.find_element_by_xpath(f"""//*[contains(text(),'{text}')]""")

def go_enter(driver, target, coords, strn):
    action = webdriver.common.action_chains.ActionChains(driver)
    action.move_to_element(target)
    action.move_by_offset(*coords)
    action.click()
    action.send_keys(strn)
    action.send_keys(Keys.RETURN)
    action.perform()
    random_sleep(1.0)

    
# close the now available pop up if it exists
def close_pop_up_access_search_bar(driver):
    try:
        element=get_element_contains(driver,'hidden label')
        actions = ActionChains(driver)
        actions.move_to_element(element).click().perform()
        
        #after closing the pop up - only one TAB is needed
        actions = ActionChains(driver)
        actions.send_keys(Keys.TAB)
        actions.perform()
        
    except:
        #if there is no pop up - 7 TABS are needed
        actions = ActionChains(driver)
        actions.send_keys(Keys.TAB)
        actions.perform()
        random_sleep(0.5)
        actions.perform()
        random_sleep(0.5)
        actions.perform()
        random_sleep(0.5)
        actions.perform()
        random_sleep(0.5)
        actions.perform()
        random_sleep(0.5)
        actions.perform()
        random_sleep(0.5)
        actions.perform()
        random_sleep(0.5)

#1 define options - Open the driver - in the meantime - will just be done via chrome driver manager
capabilities = {'browserName': 'chrome'}
options = webdriver.ChromeOptions()
prefs = {}
downloadPath='/.'
prefs["profile.default_content_settings.popups"]=0
prefs["download.default_directory"]=downloadPath
options.add_experimental_option("prefs", prefs)


#2 login into facebook
def login(driver, loginURL, loginName, loginPass):
    print("Navigating to login page...")
    try:
        driver.get(loginURL)
    except exceptions.WebDriverException:
        raise ValueError("No login page found!")
    print("Navigated to login page.")

    print("Logging in...")

    random_sleep(0.5)
    username = driver.find_element_by_id("email")
    password = driver.find_element_by_id("pass")
    username.send_keys(loginName)
    random_sleep(0.2)
    password.send_keys(loginPass)
    random_sleep(0.2)
    
    submit = driver.find_element_by_name("login")
       
    submit.click()
    random_sleep(5)
    
    print("Logged in.")

def test():
    driver = webdriver.Remote(command_executor = vm_public_ip, desired_capabilities = capabilities, options= options)
    driver.fullscreen_window()
    login(driver, loginURL, loginName, loginPass)
    

with DAG(
    ## MANDATORY 
    dag_id='sitrep_scrapper_connectivity',
    start_date=datetime(2022,11,28),
    default_args=default_args,
    description='sitrep scrapper connectivity',
    #schedule not used for the moment as the DAGS run when airflow boots everymorning
    #schedule_interval='0 2 * * *',
    # no need to catch up on the previous runs
    catchup=False
) as dag:

        test = PythonOperator(
            task_id="login",
            python_callable= test
            )

    
