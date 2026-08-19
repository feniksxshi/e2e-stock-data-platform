import json 
import logging 
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from minio import Minio 
from io import BytesIO 

logger = logging.getLogger(__name__)

def _get_minio_client():
	return S3Hook(aws_conn_id="minio", verify=False) 

def _store_data_to_bucket(data, path, bucket_name):
	client = _get_minio_client()
	if not client:
		logger.error("Failed to connect to MinIO")
		raise 

	payload = json.dumps(data, indent=4, ensure_ascii=False).encode('utf8')
	client.load_bytes(
		bytes_data=payload,
        key=f'{path}',
        bucket_name=bucket_name,
        replace=True
	)

	logger.info("Uploaded to s3a://%s/%s, size=%s bytes",
		bucket_name,
		f'{path}',
		len(payload))
	