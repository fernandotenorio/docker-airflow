from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime
import urllib.request
import json


def get_data():
	url = 'http://flask-nginx/prediction/api/v1.0/getdata'
	data = {'params': ['param_0', 'param_1', 'param_n']}
	data = json.dumps(data)
	data = data.encode('utf-8')

	req = urllib.request.Request(url)
	req.add_header('Content-Type', 'application/json; charset=utf-8')
	req.add_header('Content-Length', len(data))

	response = urllib.request.urlopen(req, data)
	response = response.read().decode('utf-8')
	response = json.loads(response)
	print(response)
	

def proc_data():
	url = 'http://flask-nginx/prediction/api/v1.0/procdata'
	data = {'params': ['param_0', 'param_1', 'param_n']}
	data = json.dumps(data)
	data = data.encode('utf-8')

	req = urllib.request.Request(url)
	req.add_header('Content-Type', 'application/json; charset=utf-8')
	req.add_header('Content-Length', len(data))

	response = urllib.request.urlopen(req, data)
	response = response.read().decode('utf-8')
	response = json.loads(response)
	print(response)

def train_model():
	url = 'http://flask-nginx/prediction/api/v1.0/train'
	data = {'params': ['param_0', 'param_1', 'param_n']}
	data = json.dumps(data)
	data = data.encode('utf-8')

	req = urllib.request.Request(url)
	req.add_header('Content-Type', 'application/json; charset=utf-8')
	req.add_header('Content-Length', len(data))

	response = urllib.request.urlopen(req, data)
	response = response.read().decode('utf-8')
	response = json.loads(response)
	print(response)

 
with DAG('train_api', description='Python DAG', schedule_interval='*/1 * * * *', start_date=datetime(2020,3,10), catchup=False) as dag:
	ini_task = DummyOperator(task_id='ini_task', retries=3)
	get_data_task = PythonOperator(task_id='get_data', python_callable=get_data)
	proc_data_task = PythonOperator(task_id='proc_data', python_callable=proc_data)
	train_model_task = PythonOperator(task_id='train_model', python_callable=train_model)
	end_task = DummyOperator(task_id='end_task')
	ini_task >> get_data_task >> proc_data_task >> train_model_task >> end_task