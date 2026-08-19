import pendulum
import logging 
import requests 
from datetime import timedelta, date
from airflow.sdk import dag, task
from airflow.providers.http.sensors.http import HttpSensor 
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.timetables.interval import CronDataIntervalTimetable
from utils.resolve_source_date import resolve_source_date

logger = logging.getLogger(__name__)

TZ = pendulum.timezone("UTC") 
DEFAULT_ARGS = {
    "owner": "hienfeang", 
    "retries": 3,
	"retry_delay": timedelta(minutes=1),
	"retry_exponential_backoff": True,
	"max_retry_delay": timedelta(minutes=10)
}
@dag(
	dag_id='py-el-api-news-sentiment',
	default_args=DEFAULT_ARGS,
	start_date=pendulum.datetime(2026, 1, 3, tz=TZ),
	schedule=CronDataIntervalTimetable(
		"30 0 * * *",
		timezone=TZ
	),
	catchup=False,
 	dagrun_timeout=timedelta(hours=1),
	max_active_runs=1, # Avoid exceeding the maximum requests per minute
	tags=[
		'egn:python', 
		'scr:alphavantage-api', 
		'dom:stock-market'
	]
)
def EL_news_sentiment_daily():
	from utils.store_data import _store_data_to_bucket 

	start = EmptyOperator(task_id='start')
	finish = EmptyOperator(task_id='finish')

	check_api = HttpSensor(
		task_id=f'is_alphavantage_api_available',
		http_conn_id='alphavantage_api',
		endpoint='query',
		method='GET',
		request_params={
			"function": "NEWS_SENTIMENT",
			"tickers": "AAPL",
		},
		response_check=lambda response: response.status_code == 200,
		poke_interval=30,
		timeout=120,
		mode='reschedule'
	)

	@task(task_id='fetch_news_sentiment')
	def fetch_news_sentiment(
		start_ts: str, 
	 	end_ts: str, 
	  	source_date: str
	):
		logger.info(
			"Extracting source_date=%s, interval_start=%s, interval_end=%s",
			source_date,
			start_ts,
			end_ts
		)

		conn = BaseHook.get_connection('alphavantage_api')
		api_key = conn.extra_dejson.get('api_key')
		url = f'{conn.schema}://{conn.host}/query'
		# Alpha Vantage requires: 20260818T0000
		api_source_date = date.fromisoformat(source_date).strftime("%Y%m%d")
		params = {
			"function": "NEWS_SENTIMENT",
			"time_from": f"{api_source_date}T0000",
			"time_to": f"{api_source_date}T2359",
			"sort": "LATEST",
			"limit": "1000",
			"apikey": api_key
		}
		
		response = requests.get(url=url, params=params, timeout=60)	
		if response.status_code == 429 or response.status_code >= 500:
			raise AirflowException(
				f"Retryable API error: status_code={response.status_code}"
			)
		if response.status_code != 200:
			raise AirflowFailException(
				f"Non-retryable API error: "
				f"status_code={response.status_code}, "
				f"response={response.text}"
			)
   
		data = response.json()
		if not data or data.get('items', 0) == 0:
			raise AirflowFailException(f"Empty payload")
		
		path = f'news_sentiment/source_date={source_date}/data.json'
		_store_data_to_bucket(data, path, 'landing')

		logger.info("Stored %s news to landing path=%s", data['items'], path)

	resolved_source_date = resolve_source_date()
	fetch_task = fetch_news_sentiment(
		start_ts='{{ data_interval_start }}',
		end_ts='{{ data_interval_end }}',
		source_date=resolved_source_date
	)
	start >> resolved_source_date >> check_api >> fetch_task >> finish
		
EL_news_sentiment_daily()	
		




