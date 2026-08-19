
import pendulum
import logging 
import requests 
from datetime import timedelta
from airflow.sdk import dag, task
from airflow.providers.http.sensors.http import HttpSensor 
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
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
	dag_id='py-el-api-eod-ohlc-prices',
	default_args=DEFAULT_ARGS,
	start_date=pendulum.datetime(2026, 1, 3, tz=TZ),
	schedule=CronDataIntervalTimetable(
		"30 0 * * *",
		timezone=TZ
    ),
	catchup=False,
	max_active_runs=1, # Avoid exceeding the maximum requests per minute
	tags=[
		'egn:python', 
		'scr:massive-api', 
		'dom:stock-price'
	]
)
def EL_eod_ohlc_prices_daily():
	from utils.store_data import _store_data_to_bucket 
	
	start = EmptyOperator(task_id='start')
	finish = EmptyOperator(task_id='finish')

	check_api = HttpSensor(
		task_id=f'is_massive_api_available',
		http_conn_id='massive_api',
		endpoint='v2/aggs/grouped/locale/us/market/stocks/2026-01-01',
		method='GET',
		response_check=lambda response: response.status_code == 200,
		poke_interval=30,
		timeout=120,
		mode='reschedule'
	)

	@task(task_id='fetch_eod_ohlc_prices')
	def fetch_eod_ohlc_prices(
    	start_ts: str, 
     	end_ts: str,
      	source_date: str
    ):
		fmt_date = pendulum.parse(source_date).date()
		if fmt_date.day_of_week in (pendulum.SATURDAY, pendulum.SUNDAY):
			raise AirflowSkipException(
       			f'{fmt_date} is not a business date (num_date={fmt_date.day_of_week}), no market data available',
          	)

		logger.info(
			"Extracting source_date=%s, interval_start=%s, interval_end=%s",
			source_date,
			start_ts,
			end_ts
		)
		
		conn = BaseHook.get_connection('massive_api')
		headers = conn.extra_dejson
		endpoint=f"v2/aggs/grouped/locale/us/market/stocks/{source_date}"
		url = f'{conn.schema}://{conn.host}/{endpoint}'
  
		response = requests.get(url=url, headers=headers, timeout=60)		
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
		if data['queryCount'] == 0:
			raise AirflowFailException('Empty payload')
		
		path = f"EOD_stock_prices/incremental/market=us/source_date={source_date}/data.json"
		_store_data_to_bucket(data, path, 'landing') 

		logger.info(
			"Stored EOD prices for %s tickers to landing path=%s at date=%s", 
			len(data.get("results", [])),
			path,
			source_date
		)

	resolved_source_date = resolve_source_date()
	fetch_task = fetch_eod_ohlc_prices(
		start_ts='{{ data_interval_start }}',
		end_ts='{{ data_interval_end }}',
		source_date=resolved_source_date
	)
	start >> resolved_source_date >> check_api >> fetch_task >> finish

EL_eod_ohlc_prices_daily()