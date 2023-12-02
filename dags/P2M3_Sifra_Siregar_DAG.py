"""


Name: Sifra Siregar


This program automates the process of fetching, cleaning, and uploading data from a database to an Elasticsearch index using Airflow.
"""

"2. IMPORT LIBARIES"

import pandas as pd
import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

"3. FUNCTION - GET DATA"

def getdata():
    ''' 
    This function is to retrieve the data from a specific table in PostgreSQL database and save it to SQL.
     
    Parameters: 
     - host: the database server address
     - port: the port number on the database server
     - user: the username for database access
     - password: the password for database access
     - database: name of the database to connect to
      
    Data Save:
     - the data will be saved to csv file
      
    Example of Usage: 
     - this project will be using function called 'getdata' to grab the data information from a table named 'airflow' in a database and will be saved
       on a file called 'data_raw.csv'
      '''
    conn = psycopg2.connect(
            host="postgres",
            port="5432",
            user="airflow",
            password="airflow",
            database="airflow"
        )
    data = pd.read_sql ('SELECT * FROM table_m3', conn)
    data.to_csv('data_raw.csv', index= False)

"4. CLEAN DATA"
def clean(df_path):
    ''' 
    To clean and prepare a dataset from a CSV file for analysis by selecting specific columns, adjusting data types, 
    and removing duplicates and missing values, then saving the cleaned data to a new CSV file.
    
    Parameters:
    'df_path': the file path of the CSV file containing the dataset to be cleaned

    Process:
    - read csv file: load csv files to a dataframe called 'data'
    - changes column names: replaces spaces with underscores in column names and converts column names to lowercase.
    - select specifies column: keeps the only columns that will be on further analysis 
    - change data type: changes the types of data to make sure they are consistent and suitable for analysis.
    - remove duplicates: drops duplicate rows from the DataFrame.
    - remove missing values: drops rows with any missing values.
    - save cleaned data to CSV: the cleaned DataFrame is saved as a new CSV file named 'data_clean.csv'

    Example: 
    - This action will be used a function called 'clean' which will be clean the data from the previous csv file and save processed data to 'P2M3_Sifra_Siregar_data_clean.csv' 
    '''
    data = pd.read_csv(df_path)

    # change columns name
    data.columns = [col.replace(' ', '_') for col in data.columns]
    data.columns = data.columns.str.lower()

    # columns to keep
    columns_to_keep = [
        'business_id', 'business_name', 'business_address',
        'business_postal_code', 'inspection_date', 'inspection_score',
        'inspection_type', 'violation_description', 'risk_category',
        'current_supervisor_districts'
    ]
    data = data[columns_to_keep]

    # drop data duplicates
    # Assuming df is your DataFrame
    data = data.drop_duplicates(subset=['business_id', 'inspection_date', 'inspection_score'])


    # drop missing value
    data = data.dropna().reset_index(drop=True)
    # change data types
    data_types = {
        'business_id': 'int64',
        'business_name': 'string',
        'business_address': 'string',
        'business_postal_code': 'int',
        'inspection_date': 'string',  
        'inspection_score': 'float64',
        'inspection_type': 'string',
        'violation_description': 'string',
        'risk_category': 'string',
        'current_supervisor_districts': 'string'  
    }
    data = data.astype(data_types)

    

    #save to csv
    data.to_csv('data_clean.csv', index=False)

"5. ELASTIC SEARCH"
def index_to_elasticsearch(data_path, index_name):
    '''
    This function goes through each row of a given DataFrame and adds that row to an Elasticsearch index.
    
    Parameters:
    - data_path: the file path of the CSV file containing the data.
    - index_name: the name of the Elasticsearch index where the data will be stored.
    
    Examples:
    'index_to_elasticsearch('my_data.csv', 'my_index')' 
    - Running this will read the data from 'my_data.csv' and upload each row to the 'my_index' Elasticsearch index.
    '''
    
    
    es = Elasticsearch(hosts= 'http://elasticsearch:9200')
    df = pd.read_csv(data_path)
    for _, row in df.iterrows():
        doc = row.to_json()
        res = es.index(index="m3", body=doc)

"6. CREATE DAG"
default_args = { # Default arguments for DAG tasks
    'owner': 'sifra',
    'start_date': dt.datetime(2023, 11, 26),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=3),
}


clean_data_args = { # Argument for the `clean_data` function
    'df_path': '/opt/airflow/P2M3_Sifra_Siregar_data_raw.csv'
}

elasticsearch_args = { # Arguments for the `post_to_elasticsearch` function
    'data_path': '/opt/airflow/data_clean.csv',
    'index_name': 'from_container_m3'
}


with DAG('fetch_clean_elastic',
         default_args=default_args,
         schedule_interval='30 23 * * *', # Daily at 1130PM UTC // 630AM UTC+7
         catchup=False) as dag:
    
    connect_task = PythonOperator(
        task_id='connect',
        python_callable=getdata
    )

    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean,
        op_kwargs=clean_data_args
    )

    post_to_elasticsearch_task = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=index_to_elasticsearch,
        op_kwargs=elasticsearch_args
    )

    connect_task >> clean_data_task >> post_to_elasticsearch_task