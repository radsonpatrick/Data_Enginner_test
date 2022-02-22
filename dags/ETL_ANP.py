from datetime import date,datetime
import unidecode  
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd
import urllib
import os
import pymysql


def download_archive() :
    link_arquivo = "https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls"
    urllib.request.urlretrieve(link_arquivo, "./raw_data/vendas-combustiveis-m3.xls")  # For Python 3
    

def xls_to_ods() :
    os.system('soffice --headless --convert-to ods ./raw_data/vendas-combustiveis-m3.xls --outdir ./raw_data')

def csv_convert(TableName,sheetName):
     df = pd.read_excel('./raw_data/vendas-combustiveis-m3.ods', sheet_name=sheetName)
     df.columns = ['combustivel', 'ano', 'regiao', 'uf', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', 'total']
     df = df.melt(id_vars=['combustivel', 'ano', 'regiao', 'uf'])
     df1 = df[df['variable']!='total']
     df1['data'] = df1['ano'].astype(str) + '/' + df1['variable'] + '/28'
     df1['data'] = pd.to_datetime(df1['data'])
     df1 = df1.drop(labels=['regiao', 'ano','variable'], axis=1)
     df1.columns = ['product', 'uf', 'volume', 'year_month']
     df1['product'] = df1['product'].str.replace('(m3)',"",regex = False).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
     df1['uf'] = df1['uf'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
     df1['unit'] = 'm3'
     df1['volume'] = pd.to_numeric(df1['volume'])
     df1['create_at'] = pd.to_datetime('today')
     df1 = df1.fillna(0)
     df1.columns = ['product', 'uf', 'volume', 'year_month','unit','created_at']
     df1.to_csv( f'./raw_data/{TableName}.csv')
   
def sql_data_load(TableName):
    engine = create_engine("mysql+pymysql://{user}:{pw}@mysql/{db}"
                       .format(user="root",
                               pw="airflow",
                               db="ANP"))
    create_table = f"""
        CREATE TABLE IF NOT EXISTS {TableName} (
            id integer PRIMARY KEY,
            product VARCHAR(256),
            uf VARCHAR(256),
            volume FLOAT,
            `year_month` date,
            unit VARCHAR(256),
            created_at datetime);"""
    engine.execute(create_table)
    engine.execute(f'TRUNCATE TABLE {TableName}')
    df1 = pd.read_csv(f'./raw_data/{TableName}.csv')
    df1.columns = ['id','product', 'uf', 'volume', 'year_month','unit','created_at']
    df1.to_sql(f'{TableName}', con = engine, if_exists = 'append', chunksize = 1000,index = False)
    


with DAG("ETL_ANP",start_date = datetime(2022,1,1), schedule_interval="@daily", catchup = False , 
         description = 'Processo de ETL para extração de informações dos arquivos disponibilizados pela ANP(Agencia Nacional de Petroleo') as dag:
    


    download_archive = PythonOperator(
        task_id='download_archive', 
        python_callable=download_archive,dag=dag)

    xls_to_ods = PythonOperator(
       task_id='xls_to_ods', 
       python_callable=xls_to_ods,dag=dag)

    csv_convert_diesel = PythonOperator(
        task_id='csv_convert_diesel', 
        python_callable=csv_convert,
        op_kwargs={'sheetName': 2, 'TableName': 'diesel'},dag=dag)

    sql_data_load_diesel = PythonOperator(
         task_id='sql_data_load_diesel', 
         python_callable=sql_data_load,
         op_kwargs={'TableName': 'diesel'}) 
    
    csv_convert_dev_fuel = PythonOperator(
        task_id='csv_convert_dev_fuel', 
        python_callable=csv_convert,
        op_kwargs={'sheetName': 1, 'TableName': 'dev_fuel'},dag=dag)

    sql_data_load_dev_fuel = PythonOperator(
         task_id='sql_data_load_dev_fuel', 
         python_callable=sql_data_load,
         op_kwargs={'TableName': 'dev_fuel'}) 
    
        
download_archive>>xls_to_ods>>csv_convert_dev_fuel>>csv_convert_diesel>>sql_data_load_dev_fuel>>sql_data_load_diesel

