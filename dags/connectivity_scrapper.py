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
loginPass=${{ secrets.META_PASSWORD }}

capabilities = {'browserName': 'chrome'}

#list of countries to filter 
country_list_eapr= ["Australia",
                   "Brunei",
                   "Cambodia",
                   "Cook",
                   "Fiji",
                   "Indonesia",
                   "China",
                   #"Japan",
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

#for testing purposes only - to be remioved 
#country_list_eapr= ["Vanuatu"]

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

hex_granularity=8
def get_h3(lat,lon):
    return h3.geo_to_h3(lat=lat,lng=lon, resolution=hex_granularity)

#1 define options - Open the driver - in the meantime - will just be done via chrome driver manager

options = webdriver.ChromeOptions()
#options.add_argument("--headless=new")

prefs = {}
downloadPath='/Users/hugoruizverastegui/Documents/UNICEF - Github/hd4d_ppt/data_engineering/notebooks/downloads'
prefs["profile.default_content_settings.popups"]=0
prefs["download.default_directory"]=downloadPath
options.add_experimental_option("prefs", prefs)

from selenium import webdriver
driver = webdriver.Remote(command_executor = 'http://52.149.136.234:4444/wd/hub', desired_capabilities = capabilities, options= options)
driver.fullscreen_window()

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
    driver.save_screenshot('screenshot.png')
        
    submit = driver.find_element_by_name("login")
       
    submit.click()
    random_sleep(5)
    driver.save_screenshot('screenshot.png')
    
    print("Logged in.")
