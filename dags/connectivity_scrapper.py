

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

import matplotlib 
from matplotlib import pyplot as plt
from matplotlib import image as mpimg



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

#loginName=os.getenv('META_LOGIN')
#loginPass=os.getenv('META_PASSWORD')

loginName='huruiz@unicef.org'
loginPass='testunicef'

#GET POSTGRES INFO
password_postgres=os.getenv('POSTGRES_PASSWORD')
login_postgres=os.getenv('POSTGRES_LOGIN')

#vm_public_ip=os.getenv('VM_PUBLIC_IP')
vm_public_ip='http://172.172.230.100:4444'
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


#1 define options - Open the driver - in the meantime - will just be done via chrome driver manager
capabilities = {'browserName': 'chrome'}
options = webdriver.ChromeOptions()
prefs = {}
downloadPath='./selenium_download'
prefs["profile.default_content_settings.popups"]=0
prefs["download.default_directory"]=downloadPath
options.add_experimental_option("prefs", prefs)


#2 login into facebook
def login(driver, loginURL, loginName, loginPass):
    print("Navigating to login page...")
    driver.get(loginURL)

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

def get_datasets(driver):
    searchstr='Network Coverage Map'

    #3 - Open the data for good page
    dataUrl='https://partners.facebook.com/data_for_good/data/?partner_id=3884468314953904'
    driver.get(dataUrl)
    random_sleep(5)

    print('on the dataset page')
    htmlsource=driver.page_source
    print(re.findall('Data for Good at Meta Portal', htmlsource))

    #4 - Search for the dataset type we are interested in 
    #inc ase this does not work - typing tabs 8 times also does the trick - but dirty
    #close pop up and go to the search bar to select the dataset type we need
    close_pop_up_access_search_bar(driver)

    #5 type the dataset type 
    actions = ActionChains(driver)
    actions.send_keys(searchstr)
    random_sleep(0.5)
    actions.send_keys(Keys.ENTER)
    actions.perform()
    driver.fullscreen_window()
    driver.maximize_window()

    #6 - Get the list of all the datasets 
    # press tab 10 times in a row and wait and then press the down key 
    #driver.maximize_window()
    #define the action

    actions = ActionChains(driver)
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.TAB)
    actions.send_keys(Keys.TAB)

    random_sleep(1)
    actions.perform()

    actions = ActionChains(driver)
    actions.send_keys(Keys.ARROW_DOWN)
    #repeat the action 500 times
    i=0
    while i<500:
        random_sleep(0.2)
        actions.perform()
        i=i+1

    random_sleep(3)
    htmlsource=driver.page_source

    regexp_dataset_id="end_date\"><input type=\"hidden\" value=\"(.*?)\" name=\"dataset_id"
    dataset_id_list = re.findall(regexp_dataset_id, htmlsource)
    return dataset_id_list


def get_already_downloaded():
    # get the list of already downloaded datasets
    #to be improved

    engine = create_engine(f'postgresql://{login_postgres}:{password_postgres}@postgres-fdtn.postgres.database.azure.com:5432/postgres')
    query='select mcd.meta_disaster_id as disaster_id, mch.data_type as data_type,mch.date from private.meta_connectivity_disaster_test mcd left join private.meta_connectivity_hex_test mch on mcd.meta_disaster_id = mch.meta_disaster_id group by 1,2,3 '
    
    df_already_in_db=pd.read_sql(query,engine)
    df_already_in_db['uid']=df_already_in_db['disaster_id'].astype(str) +"-"+ df_already_in_db['data_type']+"-"+df_already_in_db['date'].astype(str)
    
    list_already_in_db=list(df_already_in_db['uid'])
    list_disaster_already_in_db=list(df_already_in_db['disaster_id'].unique())

    # to remove later
    list_already_in_db=[]
    list_disaster_already_in_db=[]
    
    return (list_already_in_db, list_disaster_already_in_db)



def download_csvs(driver,datatype):
    random_sleep(1)
    htmlsource=driver.page_source
    random_sleep(1)
    
    # get the disaster name 
    regexp_disaster_name="div aria-level=\"2.*?\" role=\"heading\">(.*?)<\/div>"
    try:
        disaster_name= re.findall(regexp_disaster_name, htmlsource)[0]
    except:
        print('no disaster name')
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
            try:
                get_element_contains(driver,csv_element).click()
                print(f"{csv_element} downloaded")
            except:
                # if error go back after getting the error
                print(f"{csv_element} - error downloading csv")
                        
        else:    
            print(f"-- element already in database {check_duplicate}")
            pass
        
    # list the files that have been downloaded
    csvfilenames =  glob.glob(os.path.join(downloadPath, "*.csv"))
    #df_concat=pd.DataFrame(columns=['value','country','lon','lat','data_type','disaster_name','disaster_id','date'])
    
    # open the files one by one
    #file_no=0
    for csvfilename in csvfilenames:
        df_csv=pd.read_csv(csvfilename)
        #get the date from the csv_name
        regexp_date="(\d{4}-\d{2}-\d{2})"
        date_dataset=re.findall(regexp_date, csvfilename)[0]
    
        #change the column name
        #data_type= [x for x in df_csv.columns if x in ('bingtiles','admin_regions') ][0]

        #create new column with the datatype and datatype id 
        df_csv['data_type']=datatype

        #create new column with the name of the disaster
        df_csv['disaster_name']=disaster_name
        df_csv['disaster_id']=dataset_id
        df_csv['date']=date_dataset

        df_concat=pd.concat([df_concat,df_csv])
        print(csvfilename+ ' concat')
    return df_concat



def scrape(dataset_id_list):
    
    #8 - for each dataset - open the url and download all the csvs 
    today_date=datetime.now()
    
    #initialization of the browser 
    driver = webdriver.Remote(command_executor = vm_public_ip, desired_capabilities = capabilities, options= options)
    
    #login initialization
    login(driver, loginURL, loginName, loginPass)
    
    #for each dataset id: for loop
    for dataset_id in dataset_id_list:
        #start by making sure the download folder is empty
        #delete everything in the download file 

        files = glob.glob(downloadPath+'/*')
        for f in files:
            os.remove(f)    
    
        # open the dataset url 
        print(f"--dataset id - {dataset_id}")
        url= f'https://partners.facebook.com/data_for_good/data/{dataset_id}/files/?partner_id=3884468314953904'
        driver.get(url)
    
        random_sleep(2)
    
        # get the disaster name 
        
        regexp_disaster_name="div aria-level=\"2.*?\" role=\"heading\">(.*?)<\/div><span class=\""
        try:
            disaster_name= re.findall(regexp_disaster_name, htmlsource)[0]
        except:
            disaster_name=''
            print(htmlsource)
        print('disaster name: ' +disaster_name)
        
        
        # once we have the disaster name - check if it's in the EAPR region
        # if not move to the next disaster and stop the computing
    
        if is_in_eapr(disaster_name)==0 :
            print(f"-- disaster out of EAPR boundaries {disaster_name}")
            pass
    
        else:
    
            # identify all the csvs and download them - first for the coverage
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
    
            df_concat=download_csvs(driver,datatype)
            print('concat ok for 1st type')
    
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
    
            df_concat=pd.concat([df_concat,download_csvs(driver,datatype)])
            print('concat ok for 2nd type')
            #print(len(df_concat))
    
    
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
    
            df_concat=pd.concat([df_concat,download_csvs(driver,datatype)])
            print('concat ok for 3rd type')
            #print(len(df_concat))
    
            # if there is no data in df concat, meaning the datasets have all alreay been downloaded, we just stop
            if len(df_concat)>0:
                print(len(df_concat))
                #get h3 for the lat lon 
                df_concat['h3_08']=df_concat.apply(lambda x: get_h3(x['lat'], x['lon']), axis=1)
                print('h3 ok')
                
                #drop lat/lon
                df_concat=df_concat.drop(columns=['lat','lon'])
    
                #group by h3_08 and datatype - mean of the value
                df_concat_backup=df_concat
                df_concat=df_concat.groupby(['disaster_name','disaster_id','country','data_type','date', 'h3_08']).mean().reset_index()
                
                #add update date 
                df_concat['update_date']=today_date
    
                #change the column names to align with the sql 
                df_concat=df_concat.rename(columns={"disaster_id": "meta_disaster_id", "disaster_name": "meta_disaster_name"})
    
                df_concat_hex=df_concat[['meta_disaster_id','country','data_type','date', 'h3_08','update_date','value']]
    
                #save into postgres connectivity_hex
                engine = create_engine(f'postgresql://{login_postgres}:{password_postgres}@postgres-fdtn.postgres.database.azure.com:5432/postgres')
                print('engine created')
    
                df_concat_hex.to_sql('meta_connectivity_hex_test_dag', if_exists='append' ,schema='private',con=engine,chunksize=500000, method='multi', index=False)
                print('df_concat_hex - saved into postgres')
                #save into postgres connectivity_disaster
                # only if there is a new disaster
    
                if dataset_id not in list_disaster_already_in_db:
                    df_concat_disaster=df_concat[['meta_disaster_name','meta_disaster_id']].groupby(['meta_disaster_name','meta_disaster_id']).count()
                    print(df_concat_disaster)
    
                    df_concat_disaster.to_sql('meta_connectivity_disaster_test_dag', if_exists='append',schema='private',con=engine,chunksize=500000, method='multi')
                    print('df_concat_disaster - saved into postgres')
    
            #randomly regenerate a new browser to avoid detection by meta and re login 
            rand_browser_int= randint(1,10) 
            if rand_browser_int >= 7:
                print('closing browser')
                #close the driver
                driver.quit()
                # and reopen a new one + login
                driver = webdriver.Remote(command_executor = vm_public_ip, desired_capabilities = capabilities, options= options)
    
                login(driver, loginURL, loginName, loginPass)
                #stop the driver 
            
            random_sleep(5)



def test():
    options.add_argument("--headless")
    driver = webdriver.Remote(command_executor = vm_public_ip, desired_capabilities = capabilities, options= options)
    driver.fullscreen_window()
    login(driver, loginURL, loginName, loginPass)
    driver.save_screenshot('screenshot_login.png')
    
    image = mpimg.imread("screenshot_login.png")
    plt.imshow(image)
    plt.show()
    print(plt.show())    

    dataUrl='https://partners.facebook.com/data_for_good/data/?partner_id=3884468314953904'
    driver.get(dataUrl)

    htmlsource=driver.page_source
    print(re.findall('sufficient privi', htmlsource))
    
    #dataset_id_list=get_datasets(driver)
    #print(dataset_id_list) 
    scrape(['324470226907150','324470226907150'])



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

    
