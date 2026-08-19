from datetime import date

from airflow.sdk import task, get_current_context
from airflow.exceptions import AirflowFailException


@task(task_id='resolve_source_date')
def resolve_source_date(timezone_name: str = "UTC") -> str:
	"""
	Needed for manual corrections and controlled reprocessing
	Important note: A scheduled run normally has no 'source_date' in 'dag_run.conf'

	Case:
	- 2026-08-19 00:30 - DAG loads stock data for 2026-08-18
	- 2026-08-21 10:00 - Discover 2026-08-18 API response was incomplete
	- The manual DAG trigger should use: source_date=2026-08-18
	"""

	context = get_current_context()
	run_conf = context["dag_run"].conf or {}

	source_date_override = run_conf.get("source_date")
	if source_date_override is not None:
		try:
			return date.fromisoformat(source_date_override).isoformat()  # 'YYYY-MM-DD'
		except (TypeError, ValueError) as e:
			raise AirflowFailException(
				'dag_run.conf.source_date must use YYYY-MM-DD format'
			) from e

	# A normally scheduled run has 'data_interval_start'
	data_interval_start = context.get("data_interval_start")
	if data_interval_start is None:
		raise AirflowFailException(
			"No data interval is available. Please supply source_date manually"
		)
	return (
		data_interval_start
		.in_timezone(timezone_name)
		.date()
		.isoformat()
	)