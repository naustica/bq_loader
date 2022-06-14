from google.cloud import bigquery, storage
from google.cloud.bigquery import LoadJobConfig, SourceFormat
from google.api_core.exceptions import BadRequest
from multiprocessing import cpu_count
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import glob
from dataclasses import dataclass
from .utils import source_format_validator, write_disposition_validator, print_progress


@dataclass
class JobConfig:
    project_id: str
    dataset_id: str
    schema_file_path: str
    source_format: str
    csv_field_delimiter: str = ','
    csv_quote_character: str = '"'
    csv_allow_quoted_newlines: bool = False
    csv_skip_leading_rows: int = 0
    write_disposition: str = bigquery.WriteDisposition.WRITE_EMPTY
    table_description: str = ''
    ignore_unknown_values: bool = False

    @property
    def client(self):
        client = bigquery.Client()
        return client

    @property
    def dataset(self):
        dataset = bigquery.Dataset(f'{self.project_id}.{self.dataset_id}')
        return dataset

    @property
    def config(self):
        source_format = source_format_validator(self.source_format)
        write_disposition = write_disposition_validator(self.write_disposition)

        job_config = LoadJobConfig()

        job_config.source_format=source_format
        job_config.write_disposition = write_disposition
        job_config.ignore_unknown_values=self.ignore_unknown_values
        job_config.schema=self.client.schema_from_json(self.schema_file_path)
        job_config.destination_table_description = self.table_description

        if source_format == SourceFormat.CSV:
            job_config.field_delimiter = self.csv_field_delimiter
            job_config.quote_character = self.csv_quote_character
            job_config.allow_quoted_newlines = self.csv_allow_quoted_newlines
            job_config.skip_leading_rows = self.csv_skip_leading_rows

        return job_config


def create_table_from_local(table_id: str,
                            project_id: str,
                            dataset_id: str,
                            file_path: str,
                            schema_file_path: str,
                            source_format: str,
                            csv_field_delimiter: str = ',',
                            csv_quote_character: str = '"',
                            csv_allow_quoted_newlines: bool = False,
                            csv_skip_leading_rows: int = 0,
                            write_disposition: str = bigquery.WriteDisposition.WRITE_EMPTY,
                            table_description: str = '',
                            ignore_unknown_values: bool = False) -> None:
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

    job_config = JobConfig(project_id=project_id,
                           dataset_id=dataset_id,
                           schema_file_path=schema_file_path,
                           source_format=source_format,
                           csv_field_delimiter=csv_field_delimiter,
                           csv_quote_character=csv_quote_character,
                           csv_allow_quoted_newlines=csv_allow_quoted_newlines,
                           csv_skip_leading_rows=csv_skip_leading_rows,
                           write_disposition=write_disposition,
                           table_description=table_description,
                           ignore_unknown_values=ignore_unknown_values)

    client = job_config.client
    dataset = job_config.dataset

    jobs = []

    files = glob.glob(file_path)

    if not files:
        raise FileNotFoundError('No such file or directory: {0}'.format(file_path))

    for file in files:
        with open(os.path.abspath(file), 'rb') as source_file:
            job = client.load_table_from_file(source_file,
                                              dataset.table(table_id),
                                              job_config=job_config.config)

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
                             csv_field_delimiter: str = ',',
                             csv_quote_character: str = '"',
                             csv_allow_quoted_newlines: bool = False,
                             csv_skip_leading_rows: int = 0,
                             write_disposition: str = bigquery.WriteDisposition.WRITE_EMPTY,
                             table_description: str = '',
                             ignore_unknown_values: bool = False) -> None:
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

    job_config = JobConfig(project_id=project_id,
                           dataset_id=dataset_id,
                           schema_file_path=schema_file_path,
                           source_format=source_format,
                           csv_field_delimiter=csv_field_delimiter,
                           csv_quote_character=csv_quote_character,
                           csv_allow_quoted_newlines=csv_allow_quoted_newlines,
                           csv_skip_leading_rows=csv_skip_leading_rows,
                           write_disposition=write_disposition,
                           table_description=table_description,
                           ignore_unknown_values=ignore_unknown_values)

    client = job_config.client
    dataset = job_config.dataset

    load_job = None

    try:
        load_job = client.load_table_from_uri(uri,
                                              dataset.table(table_id),
                                              job_config=job_config.config)

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

    files = glob.glob(file_path)

    if not files:
        raise FileNotFoundError('No such file or directory: {0}'.format(file_path))

    with ProcessPoolExecutor(max_workers=max_processes) as executor:
        futures = []
        for file in files:
            blob_name = f'{gcb_dir}/{file}'
            future = executor.submit(
                upload_file_to_bucket,
                bucket_name,
                blob_name,
                file_path=os.path.abspath(file))
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
