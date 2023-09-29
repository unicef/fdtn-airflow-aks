

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

from selenium import webdriver

loginURL='https://www.facebook.com'
loginName=os.getenv('META_LOGIN')
loginPass=os.getenv('META_PASSWORD')

credential = DefaultAzureCredential()

options = webdriver.ChromeOptions()
options.add_argument("--headless")

capabilities = {'browserName': 'chrome',
               'platform': 'LINUX'}

prefs = {}
downloadPath='/home/hugo.ruiz.verastegui/selenium_download'
prefs["profile.default_content_settings.popups"]=0
prefs["download.default_directory"]=downloadPath
options.add_experimental_option("prefs", prefs)



## Arguments applied to the tasks, not the DAG in itself 
default_args={
    'owner':'airflow',
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':2,
    'retry_delay': timedelta(minutes=1)
}


def delete_files_vm(folder_path_to_delete):

    run_command_parameters = {
        'command_id': 'RunShellScript', # For linux, don't change it
        'script': [
            f'rm -v {folder_path_to_delete}/*.csv']
        }

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
        random_sleep(0.5)
        driver.fullscreen_window()
        
        
    except:
        pass
    
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


# copy csv files into the blob storage via azcopy
# copies everything in the selenium_download folder into the blob storage

def copy_csv_to_blob(destination_folder):

    run_command_parameters = {
        'command_id': 'RunShellScript', # For linux, don't change it
        'script': [
            'azcopy login --identity',
            f'azcopy copy "/home/hugo.ruiz.verastegui/selenium_download/*" "https://fdtnstorage.blob.core.windows.net/meta/{destination_folder}"' ]
        }


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
    driver.save_screenshot('screenshot_login.png')
    
    print("Logged in.")

def download_csvs(driver,datatype):
    random_sleep(1)
    htmlsource=driver.page_source
    random_sleep(1)
    
    # get the disaster name 
    regexp_disaster_name="div aria-level=\"2.*?\" role=\"heading\">(.*?)<\/div><span class=\""
    try:
        disaster_name= re.findall(regexp_disaster_name, htmlsource)[0]
    except:
        disaster_name=''
        
    #download all csvs in the page
    #get all the strings to be used to find the csv links via get_element_contains
    #the format is something like this: "mar 15, 2023 00:00 • 0.5 MB • csv"
    regexp_download_elements="href=\"\#\">(.*?)<\/a>"
    elements_list= re.findall(regexp_download_elements, htmlsource)
    
    #keep only elements containing csv 
    csv_elements_list = [x for x in elements_list if "csv" in x]
    print(csv_elements_list)
    
    # keep only elements that are not already in the list 
    
    #For each element in the csv_element_list -> select it and click to download  
    for csv_element in csv_elements_list: 
        
        #skip if the csv element has already been downloaded before 
        
        #extract the date from the csv name 
        regexp_date="(.*?) \d{2}\:\d{2}"
        csv_date=re.findall(regexp_date,csv_element)[0]
        csv_date=parser.parse(csv_date).date()
        csv_date=str(csv_date)
        check_duplicate=dataset_id+"-"+datatype+"-"+csv_date
        
        
        if check_duplicate not in list_already_in_db:
            random_sleep(5)
            #click on the button to download the csv
            get_element_contains(driver,csv_element).click()
            random_sleep(4)
            print(f"{csv_element} downloaded")
        
        else:    
            print(f"-- element already in database {check_duplicate}")
            pass

def test():
    credential = DefaultAzureCredential()
    resource_group = 'LINUX-VM_GROUP'
    vm_name='linux-vm'
    subscription_id = 'cf59aa4a-a61e-4280-9315-5c366a03d507'
    today_date=datetime.now()
    compute_client = ComputeManagementClient(
        credential=credential,
        subscription_id=subscription_id
    )
    
    #
    dataset_id_list=[
    '1042562333595133',
     '6486615568122561',
     '217166884391274',
     '232527099749686',
     '1390365964843392',
     '324470226907150',
     '1027670454913909',
     '652036090324810',
     '1361942264397012',
     '827574665583717',    
     '1650701188746310',
     '296092926384504',
     '629626665905269',
     '154706087659689',
     '824218302753800',
     '591002899775761',
     '306846535160138',
     '167228093056313',
     '1071010774270000',
     '310316941402088',
     '6777882238922828',
     '596605822639984',
     '942511623491430',
     '6455682497882256',
     '646503847464491',
     '6489118237845239',
     '270060899098738',
     '1010203409992915',
     '806579664280836',
     '2039656549705801',
     '582075167430211',
     '148854274873308',
     '1229845774359221',
     '794192338991156',
     '1710248299427830',
     '1401304900445762',
     '215591488059258',
     '620000793419250',
     '797844995131485',
     '1605104736640597',
     '979116766870812',
     '259656560078365',
     '1428907237897779',
     '3287944301426323',
     '3478359829079559',
     '1320179898925446',
     '218489137655311',
     '1141305457260882',
     '244819074864505',
     '979713546798930',
     '613677587361502',
     '6170369322998265',
     '624926539676092',
     '251962477214800']

    #turn vm on and wait 5mn 
    compute_client.virtual_machines.begin_start(resource_group, vm_name)
    time.sleep(5*60)

    #initialization of the browser 
    driver = webdriver.Remote(command_executor = 'http://172.172.230.100:4444', desired_capabilities = capabilities, options= options)
    
    #login initialization
    login(driver, loginURL, loginName, loginPass)
    
    token_delete=1
    
    #for each dataset id: for loop
    for dataset_id in dataset_id_list[0:10]:
        #try:
    
        #start by making sure the download folder on the VM is empty
        #delete everything in the download file 
        if token_delete==1:
            delete_files_vm(downloadPath)
    
        # open the dataset url 
        print(f"--dataset id - {dataset_id}")
        url= f'https://partners.facebook.com/data_for_good/data/{dataset_id}/files/?partner_id=3884468314953904'
        driver.get(url)
    
        random_sleep(2)
    
        # get the disaster name 
        htmlsource=driver.page_source
        regexp_disaster_name="div aria-level=\"2.*?\" role=\"heading\">(.*?)<\/div><span class=\""
        try:
            disaster_name= re.findall(regexp_disaster_name, htmlsource)[0]
        except:
            disaster_name=''
        print(disaster_name)
    
        # once we have the disaster name - check if it's in the EAPR region
        # if not move to the next disaster and stop the computing
    
        if is_in_eapr(disaster_name)==0 :
            print(f"-- disaster out of EAPR boundaries {disaster_name}")
            token_delete=0
            pass
    
        else:
            # identify all the csvs and download them - first for the coverage
            token_delete=1
            print ("---- Active coverage")
            random_sleep(2)
            htmlsource=driver.page_source
    
            #get the datatype from the button
            regexp_datatype="Network coverage: <strong>(.*?)</strong>"
            datatype_from_regexp=re.findall(regexp_datatype,htmlsource)[0]
            print(f'datatype from regexp: {datatype_from_regexp}')
    
            #translate the datatype into the real one used in the csvs
    
            if datatype_from_regexp=='Active':
                datatype='coverage'
            if datatype_from_regexp=='Undetected':
                datatype='no_coverage'
            if datatype_from_regexp=='Probability':
                datatype='p_connectivity'
    
            download_csvs(driver,datatype)
            print('download ok for 1st type')
     
    
    
            # then for the undetected 
            # move to the no coverage and download the csvs
            random_sleep(2)
            #find the network coverage button and click it
            get_element_contains(driver,'Network coverage').click()
    
            #move up to go to the probability data
            actions = ActionChains(driver)
            actions.send_keys(Keys.UP)
            actions.send_keys(Keys.ENTER)
            actions.perform()
    
            #go back to the files list 
            random_sleep(2)
            #go back to the files tab
            driver.find_element_by_xpath("//div[@aria-label='Files']").click()
    
            #concat the results with the previous results
            print ("---- 2nd data type")
    
            random_sleep(2)
            htmlsource=driver.page_source
    
            #get the datatype from the button
            regexp_datatype="Network coverage: <strong>(.*?)</strong>"
            datatype_from_regexp=re.findall(regexp_datatype,htmlsource)[0]
            print(f'datatype from regexp: {datatype_from_regexp}')
    
            #translate the datatype into the real one used in the csvs
    
            if datatype_from_regexp=='Active':
                datatype='coverage'
    
            if datatype_from_regexp=='Undetected':
                datatype='no_coverage'
    
            if datatype_from_regexp=='Probability':
                datatype='p_connectivity'
    
            download_csvs(driver,datatype)
            print('download for 2nd type')
    
    
            # and finally for the probability 
            # first go back to the first page to reinitialize the cursor position - there has been issues of labels placed differently for each disaster
            driver.get(url)
            random_sleep(2)
            # move to the probability page and download the csvs
    
            #find the button and click it
            get_element_contains(driver,'Network coverage').click()
    
            #move up to go to the No coverage data
            actions = ActionChains(driver)
            actions.send_keys(Keys.DOWN)
            actions.send_keys(Keys.ENTER)
            actions.perform()
    
            #go back to the files list 
            random_sleep(3)
    
            #go back to the files tab
            driver.find_element_by_xpath("//div[@aria-label='Files']").click()
    
            #concat the results with the previous results
            print ("---- 3rd datatype")
    
            random_sleep(2)
            htmlsource=driver.page_source
    
            #get the datatype from the button
            regexp_datatype="Network coverage: <strong>(.*?)</strong>"
            datatype_from_regexp=re.findall(regexp_datatype,htmlsource)[0]
            print(f'datatype from regexp: {datatype_from_regexp}')
            #translate the datatype into the real one used in the csvs
    
            if datatype_from_regexp=='Active':
                datatype='coverage'
            if datatype_from_regexp=='Undetected':
                datatype='no_coverage'
            if datatype_from_regexp=='Probability':
                datatype='p_connectivity'
    
            download_csvs(driver,datatype)
            print('download ok for 3rd type')
            
            #move the csvs to the blob storage
            folder_name=disaster_name+"-"+dataset_id
            copy_csv_to_blob(folder_name)
            print("moved to blob storage")
    
    
            random_sleep(5)
            
    #stop VM 
    compute_client.virtual_machines.begin_power_off(resource_group, vm_name)



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

    
