"""
This script verifies:
- The environment value exists
- Airflow can deserialize it
- BaseHook can resolve it into a Connection object
"""
import os
import sys
import json

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException, AirflowException

CONNECTION_IDS = (
	"minio",
	"sec_api",
	"massive_api",
	"alphavantage_api"
)

def check_connection(connection_id: str) -> bool:
	env = f"{connection_id.upper()}"
	try:
		conn = BaseHook.get_connection(env) 
	except AirflowNotFoundException:
		print (
			f"[FAIL] {connection_id}: connection not found",
			file=sys.stderr
		)
		return False
	except AirflowException as error:
		print (
			f"[FAIL] {connection_id}: {type(error).__name__}",
			file=sys.stderr
		)
	
	print(f"[OK] {connection_id}: type={conn.conn_type}")
	return True

def main() -> int: 
    results = [check_connection(c) for c in CONNECTION_IDS]
    
    failed_count = results.count(False)
    successful_count = len(results) - failed_count
    
    print(
		f"Checked {len(results)} configurations: "
		f"{successful_count} passed, {failed_count} failed"
	)
    
    return 0 if failed_count == 0 else 1

if __name__ == "__main__":
    raise SystemExit(main())
    
    

