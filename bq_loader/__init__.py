from google.cloud import bigquery, storage
from google.cloud.bigquery import LoadJobConfig
from google.api_core.exceptions import BadRequest
import multiprocessing
from multiprocessing import BoundedSemaphore, cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from .utils import source_format_validator, write_disposition_validator


def create_table_from_local(table_id: str,
                            project_id: str,
                            dataset_id: str,
                            file_paths: str,
                            schema_file_path: str,
                            source_format: str,
                            write_disposition: str,
                            table_description: str,
                            ignore_unknown_values: bool) -> None:

    source_format = source_format_validator(source_format)
    write_disposition = write_disposition_validator(write_disposition)

    client = bigquery.Client()

    dataset = bigquery.Dataset(f'{project_id}.{dataset_id}')

    job_config = LoadJobConfig()

    job_config.source_format=source_format
    job_config.ignore_unknown_values=ignore_unknown_values
    job_config.schema=client.schema_from_json(schema_file_path)
    job_config.write_disposition = write_disposition
    job_config.destination_table_description = table_description

    jobs = []

    for root, dirs, files in os.walk(os.path.abspath(file_paths)):
        for file in files:
            with open(os.path.join(root, file), 'rb') as source_file:
                job = client.load_table_from_file(source_file,
                                                  dataset.table(table_id),
                                                  job_config=job_config)

                jobs.append(job)

    for job in jobs:
        job.result()

# https://github.com/The-Academic-Observatory/observatory-platform/blob/develop/observatory-platform/observatory/platform/utils/gc_utils.py
def create_table_from_bucket(uri: str,
                             table_id: str,
                             project_id: str,
                             dataset_id: str,
                             schema_file_path: str,
                             source_format: str,
                             write_disposition: str,
                             table_description: str,
                             ignore_unknown_values: bool) -> None:

    assert uri.startswith('gs://')

    source_format = source_format_validator(source_format)
    write_disposition = write_disposition_validator(write_disposition)

    client = bigquery.Client()
    dataset = bigquery.Dataset(f'{project_id}.{dataset_id}')

    job_config = LoadJobConfig()

    job_config.source_format = source_format
    job_config.schema = client.schema_from_json(schema_file_path)
    job_config.write_disposition = write_disposition
    job_config.destination_table_description = table_description
    job_config.ignore_unknown_values = ignore_unknown_values

    load_job = None

    try:
        load_job = client.load_table_from_uri(uri,
                                              dataset.table(table_id),
                                              job_config=job_config)

        result = load_job.result()

        print(f'load bigquery table result.state={result.state}')
    except BadRequest as e:
        print(f'load bigquery table failed: {e}.')
        if load_job:
            print(f'Error collection:\n{load_job.errors}')

# https://github.com/The-Academic-Observatory/observatory-platform/blob/develop/observatory-platform/observatory/platform/utils/gc_utils.py
def upload_files_to_bucket(bucket_name: str,
                           file_paths: str,
                           gcb_dir: str,
                           max_processes: int = cpu_count()) -> None:

    with ProcessPoolExecutor(max_workers=max_processes) as executor:
        futures = []
        for root, dirs, files in os.walk(os.path.abspath(file_paths)):
            for file in files:
                blob_name = f'{gcb_dir}/{file}'
                future = executor.submit(
                    upload_file_to_bucket,
                    bucket_name,
                    blob_name,
                    file_path=os.path.join(root, file))
                futures.append(future)

        for future in as_completed(futures):
            future.result()


def upload_file_to_bucket(bucket_name: str,
                          blob_name: str,
                          file_path: str) -> None:

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_filename(file_path)

    print('File {} uploaded to {}.'.format(file_path, blob_name))
