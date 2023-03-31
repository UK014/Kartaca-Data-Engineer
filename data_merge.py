import json
from urllib import request
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import mysql.connector
import logging
import sqlite3

def start():
    logging.basicConfig(level=logging.INFO)
    logging.info("DAG Started")
def task_two():
    mydb = mysql.connector.connect(
    host = "localhost",
    user="root",
    password="12345678",
    database="KARTACA")   
    mycursor = mydb.cursor()
    mycursor.execute("INSERT INTO data_merge(countryname,countryshort,currencyname,currencyshort) SELECT country.countryname, country.short,currency.currencyname, currency.short FROM country JOIN currency ON country.id = currency.id")
    mycursor.execute("DELETE FROM country")
    mycursor.execute("DELETE FROM currency")
    mydb.commit()


def end():
    logging.basicConfig(level=logging.INFO)
    logging.info("DAG Ended")       
dag = DAG(
    'Data_Merge',
    schedule='10 10 * * *',
    start_date=datetime(2023, 3, 22),
)

task_1 = PythonOperator(
    task_id='start',
    python_callable=start,
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
    task_two()
    end()
    
    