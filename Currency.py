import json
from urllib import request
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import mysql.connector
import logging

def start():
    logging.basicConfig(level=logging.INFO)
    logging.info("DAG Started")
def task_two(url):
    with open(url, 'r') as f:
        json_data = f.read()
        dict_data = json.loads(json_data)
    return dict_data
def task_three(**dict):
    mydb = mysql.connector.connect(
    host = "localhost",
    user="root",
    password="12345678",
    database="KARTACA")   
    i = 0
    keys_list = list(dict.keys())
    values_list = list(dict.values())
    while i < len(keys_list):
        mycursor = mydb.cursor()
        sql = "INSERT INTO currency(currencyname,short) VALUES(%s,%s)"
        val = (values_list[i],keys_list[i])
        mycursor.execute(sql, val)
        mydb.commit()
        i+= 1
def end():
    logging.basicConfig(level=logging.INFO)
    logging.info("DAG Ended")       
dag = DAG(
    'Currency',
    schedule='05 10 * * *',
    start_date=datetime(2023, 3, 22),
)

task_1 = PythonOperator(
    task_id='start',
    python_callable=start,
    dag=dag,
)
task_2 = PythonOperator(
    task_id='task_two',
    python_callable=task_two,
    dag=dag,
)
task_3 = PythonOperator(
    task_id='task_3',
    python_callable=task_three,
    dag=dag,
)
task_4 = PythonOperator(
    task_id='end',
    python_callable=end,
    dag=dag,
)

if __name__ == "__main__":
    start()
    url = "currency.json"
    task_three(**task_two(url))
    end()
    
    