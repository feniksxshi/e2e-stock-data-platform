import pendulum
import logging 
import requests 

from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor 
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException 
from airflow.hooks.base import BaseHook
from airflow.timetables.interval import CronDataIntervalTimetable

logger = logging.getLogger(__name__)

# ==========================================================
# Define the DAG
# ==========================================================
DEFAULT_ARGS = {'owner': 'hienfeang', 'retries': 1}

@dag(
	dag_id='py-el-api-tickers',
	default_args=DEFAULT_ARGS,
	start_date=pendulum.datetime(2025, 1, 1),
	schedule='@monthly', # At midnight of the first day
	catchup=False,
	tags=['egn:python', 'scr:sec-api', 'dom:stock-price']
)
def EL_companies_exchanges_monthly():
	from utils.store_data import _store_data_to_bucket

	EXCHANGE = "nasdaq"
	# ==========================================================
	# Tasks
	# ==========================================================
	start = EmptyOperator(task_id='start')
	finish = EmptyOperator(task_id='finish')

	check_api = HttpSensor(
		task_id=f'is_sec_api_available',
		http_conn_id='sec_api',
		endpoint=f'mapping/exchange/{EXCHANGE}',
		method='GET',
		response_check=lambda response: response.status_code == 200,
		poke_interval=30,
		timeout=120,
		mode='reschedule' # releases the worker slot between checks
	)

	@task(task_id='fetch_nasdaq_tickers_data')
	def fetch_tickers_on_nasdaq(run_date: str):
		conn = BaseHook.get_connection('sec_api')
		headers = conn.extra_dejson
		
		url = f'{conn.schema}://{conn.host}/mapping/exchange/{EXCHANGE}'

		response = requests.get(url, headers=headers, timeout=30)

		if response.status_code != 200:
			raise AirflowFailException(
				f'No data fetched for {EXCHANGE}, '
				f'status_code={response.status_code}, response={response.text}'
			)
	
		data = response.json() # Python dict object
		if not data:
			raise AirflowFailException(f"Empty payload for exchange={EXCHANGE}")

		for ticker in range(len(data)):	
			if 'ticker' not in data[ticker]:
				raise AirflowFailException(f'Invalid payload for exchange={EXCHANGE}')
		
		path = f"tickers/nasdaq/source_date={run_date}/data.json"
		_store_data_to_bucket(data, path, 'landing') 

		logger.info("Stored %s tickers to landing path=%s", len(data), path)

	start >> check_api >> fetch_tickers_on_nasdaq('{{ ds }}') >> finish

EL_companies_exchanges_monthly()