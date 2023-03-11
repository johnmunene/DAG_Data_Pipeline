#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import the prerequisites data

import os.path
import pandas as pd
import sqlite3
import wget
import zipfile

file_path = 'https://bit.ly/416WE1X'

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.providers.google.cloud.operators.gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
import pandas as pd
import psycopg2

#initiate DAG
#conn = psycopg2.connect(host="localhost", database="mydb", user="myuser", password="mypassword")
default_args = {
    'owner': 'telecom',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 7),
    'email': ['johnmunenekaruria@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'mobile-profit',
    default_args=default_args,
    description='mobile-profit-records',
    schedule_interval=timedelta(days=1),

)

#function to extract data from web
def extract_data(file_path):
    wget.download(file_path) # pylint: disable=abstract-class-instantiated
    
    
    
    
    unzipcsvfile = zipfile.ZipFile('./dataset1_202302.zip')

    promo_df = pd.read_csv(unzipcsvfile.open('dataset1.csv'))

    payment_df = pd.read_csv(unzipcsvfile.open('dataset2.csv'))
    refund_df = pd.read_csv(unzipcsvfile.open('dataset3.csv'))
#function to transform data    
def data_transform():
    #drop if customer id is null
    promo_df2 = promo_df.dropna(subset=['customer_id','total_amount_billed'])
    payment_df2 = payment_df.dropna(subset=['customer_id','amount_paid'])
    refund_df2 = refund_df.dropna(subset=['customer_id', 'refund_amount'])

    #add promo purchase amount to payment_df2

    bill_df = promo_df[['customer_id','total_amount_billed']]

    bill_df

    refund = refund_df2[['customer_id', 'refund_amount']]

    payment_df3 = payment_df2.merge(bill_df, how="left", on=["customer_id"])    

    profit_df = payment_df3.merge(refund, how ='left', on=['customer_id'])
    
    return (profit_df)
#Function to prepare dataframe for profit calculation
def profit_calcualtion(profit_df):


    profit_df['amount_due'] = (profit_df['total_amount_billed'] + profit_df['late_payment_fee'])-profit_df['amount_paid']

    profit_df['profit'] = (profit_df['amount_paid']-profit_df['refund_amount'])+profit_df['amount_due']
    profit_df

   
    return (profit_df)
#function to prepare the csv file to load to GCS
def load_data(profit_df):
    filename = datetime.today().strftime("%Y%m%d") + '_profit.csv'
    profit_df.to_csv(filename, index=False)
    return filename


#FUNCTION TO UPLOAD THE CSV FILE TO GCS BUCKET

def upload_to_gcs(filename):
    gcs_hook = GoogleCloudStorageHook()
    gcs_hook.upload(
        bucket='mobile-profit',
        object='data/mobile/' + filename,
        filename=filename,
               
    )

    
#calling of the execution of functions using python operators
extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform = PythonOperator(
    task_id='data_transform',
    python_callable=data_transform,
    dag=dag,
)


profit = PythonOperator(
    task_id='profit_calcualtion',
    python_callable=load_data,
    dag=dag,
)
load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

#uploading the file to the gcs cloud
upload = GoogleCloudBaseOperator(
    task_id='upload_to_gcs',
   
    destination_bucket='mobile-profit',
    destination_object='archive/profit/{{ ds_nodash }}_profit.csv',
    dag=dag,
)

#the DAG process flow
extract >> transform >> profit >> load >> upload


# In[ ]:




