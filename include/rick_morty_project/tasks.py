from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from datetime import datetime
from io import StringIO
from json import dumps
import pandas as pd
import requests
import json
import ast


def _get_rick_morty_characters(url):

    api = BaseHook.get_connection('rick_morty_api')
    response = requests.get(url, headers=api.extra_dejson['headers'])
    #return json.dumps(response.json()['results'])
    return response.json()['results']

def _store_characters(characters):

    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"rick_and_morty_{now}.json"


    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_string(
        string_data=json.dumps(characters, ensure_ascii=False, indent=4),
        key=f"api_results/{filename}",
        bucket_name='aws-airflow-ingest',
        replace=True
    )

def _process_json_to_csv():
    s3 = S3Hook(aws_conn_id="aws_default")

    source_bucket = "aws-airflow-ingest"
    prefix = "api_results/"
    target_bucket = "aws-airflow-transformed"

    # Get the latest JSON file
    keys = s3.list_keys(bucket_name=source_bucket, prefix=prefix)
    json_keys = [k for k in keys if k.endswith(".json")]
    if not json_keys:
        raise ValueError(f"No JSON files found in {source_bucket}/{prefix}")
    latest_key = sorted(json_keys)[-1]

    # Read and parse JSON
    json_data = s3.read_key(key=latest_key, bucket_name=source_bucket)

    # Convert from JSON string -> Python string -> Python list
    characters_str = json.loads(json_data)  # this is still a string like "[{...}, {...}]"
    characters_list = ast.literal_eval(characters_str)  # now it's a real Python list

    # Flatten nested JSON fields
    df = pd.json_normalize(characters_list)

    # Convert DataFrame to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload to S3
    target_key = latest_key.replace(prefix, "csv_output/").replace(".json", ".csv")
    s3.load_string(
        string_data=csv_buffer.getvalue(),
        key=target_key,
        bucket_name=target_bucket,
        replace=True
    )

    print(f"Processed and uploaded CSV: {target_key}")

def _get_latest_file():
    s3 = S3Hook(aws_conn_id="aws_default")
    bucket = 'aws-airflow-transformed'
    prefix = 'csv_output/'  # Optional path inside bucket
    s3_client = s3.get_conn()

    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    all_files = objects.get('Contents', [])
    
    if not all_files:
        raise ValueError("No files found in S3 bucket.")

    latest_file = max(all_files, key=lambda x: x['LastModified'])
    s3_path = f"s3://{bucket}/{latest_file['Key']}"

    return s3_path


