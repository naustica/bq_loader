from google.cloud import bigquery, storage
from google.cloud.bigquery import LoadJobConfig
from google.api_core.exceptions import BadRequest
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from .utils import source_format_validator, write_disposition_validator, print_progress


def create_table_from_local(table_id: str,
                            project_id: str,
                            dataset_id: str,
                            file_path: str,
                            schema_file_path: str,
                            source_format: str,
                            write_disposition: str,
                            table_description: str,
                            ignore_unknown_values: bool) -> None:
    """
    This function creates a table from a local file or directory.

    Parameters
    ----------
    table_id: str
        The name of the table
    project_id: str
        The name of the project in BigQuery
    dataset_id: str
        The name of the dataset in BigQuery
    file_path: str
        The directory or file from which the table is created
    schema_file_path: str
        Path to the table schema
    source_format: str
        The file format
    write_disposition: str
        Describes whether a job should overwrite or append the existing destination table if it already exists
    table_description: str
        The table description
    ignore_unknown_values: bool
        Whether unknown values should be ignored or not
    """

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

    for root, dirs, files in os.walk(os.path.abspath(file_path)):
        for file in files:
            with open(os.path.join(root, file), 'rb') as source_file:
                job = client.load_table_from_file(source_file,
                                                  dataset.table(table_id),
                                                  job_config=job_config)

                jobs.append(job)

    for n, job in enumerate(jobs, start=1):
        print_progress(n/len(jobs))
        job.result()


def create_table_from_bucket(uri: str,
                             table_id: str,
                             project_id: str,
                             dataset_id: str,
                             schema_file_path: str,
                             source_format: str,
                             write_disposition: str,
                             table_description: str,
                             ignore_unknown_values: bool) -> None:
    """
    This function creates a table from a Google Bucket.

    Code of this function is inspired by:
    https://github.com/The-Academic-Observatory/observatory-platform/blob/develop/observatory-platform/observatory/platform/utils/gc_utils.py

    Parameters
    ----------
    uri: str
         The URI of your Google bucket
    table_id: str
        The name of the table
    project_id: str
        The name of the project in BigQuery
    dataset_id: str
        The name of the dataset in BigQuery
    schema_file_path: str
        Path to the table schema
    source_format: str
        The file format
    write_disposition: str
        Describes whether a job should overwrite or append the existing destination table if it already exists
    table_description: str
        The table description
    ignore_unknown_values: bool
        Whether unknown values should be ignored or not
    Raises
    ------
    ValueError
        If the URI does not start with 'gs://'
    BadRequest
        If the creation of the table failed
    """

    if not uri.startswith('gs://'):
        raise ValueError('URI must start with gs://')

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

        print(f'Load BigQuery table result.state={result.state}')
    except BadRequest as e:
        print(f'Load bigquery table failed: {e}.')
        if load_job:
            print(f'Error collection:\n{load_job.errors}')


def upload_files_to_bucket(bucket_name: str,
                           file_path: str,
                           gcb_dir: str,
                           max_processes: int = cpu_count()) -> None:
    """
    This function uploads files into a Google Bucket.

    Code of this function is inspired by:
    https://github.com/The-Academic-Observatory/observatory-platform/blob/develop/observatory-platform/observatory/platform/utils/gc_utils.py

    Parameters
    ----------
    bucket_name: str
         The name of your Google Bucket
    file_path: str
        The directory or file which should be uploaded
    gcb_dir: str
        The name of the destination directory in the Google Bucket
    max_processes: int
        Number of concurrent tasks
    Raises
    ------
    FileNotFoundError
        If the file_path does not exist
    """

    if not os.path.exists(file_path):
        raise FileNotFoundError(' No such directory: {0}'.format(file_path))

    with ProcessPoolExecutor(max_workers=max_processes) as executor:
        futures = []
        for root, dirs, files in os.walk(os.path.abspath(file_path)):
            for file in files:
                blob_name = f'{gcb_dir}/{file}'
                future = executor.submit(
                    upload_file_to_bucket,
                    bucket_name,
                    blob_name,
                    file_path=os.path.join(root, file))
                futures.append(future)

        for n, future in enumerate(as_completed(futures), start=1):
            print_progress(n/len(futures))
            future.result()


def upload_file_to_bucket(bucket_name: str,
                          blob_name: str,
                          file_path: str) -> None:
    """
    This function uploads a single file into a Google Bucket.

    Parameters
    ----------
    bucket_name: str
         The name of your Google Bucket
    blob_name: str
        The name of the destination file
    file_path: str
        The file which should be uploaded
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_filename(file_path)

    print('File {} uploaded to {}.'.format(file_path, blob_name))
