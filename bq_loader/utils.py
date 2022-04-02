from google.cloud import bigquery
import sys


def source_format_validator(source_format: str):
    """


    Parameters
    ----------
    source_format: str
    """
    if source_format == 'jsonl':
        source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

    elif source_format == 'avro':
        source_format = bigquery.SourceFormat.AVRO

    elif source_format == 'csv':
        source_format = bigquery.SourceFormat.CSV

    elif source_format == 'mro':
        source_format = bigquery.SourceFormat.mro

    elif source_format == 'orc':
        source_format = bigquery.SourceFormat.ORC

    elif source_format == 'parquet':
        source_format = bigquery.SourceFormat.PARQUET

    else:
        raise ValueError('Source format {0} is not implemented.'.format(source_format))

    return source_format


def write_disposition_validator(write_disposition: str):
    """


    Parameters
    ----------
    write_disposition: str
    """
    if write_disposition == 'WRITE_EMPTY':
        write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

    elif write_disposition == 'WRITE_APPEND':
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    elif write_disposition == 'WRITE_TRUNCATE':
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    else:
        raise ValueError('Type of class WriteDisposition {0} is not implemented.'.format(write_disposition))

    return write_disposition


def print_progress(progress: float) -> None:
    """
    This method prints out the current progress status.

    Parameters
    ----------
    progress : float
        Current status of the progress. Value between 0 and 1.
    """

    bar_len = 50
    block = int(round(bar_len * progress))

    text = '|{0}| {1}%'.format('=' * block + ' ' * (bar_len - block),
                               int(progress * 100))

    print(text, end='\r', flush=False, file=sys.stdout)

    if progress == 1:
        print('\n', file=sys.stdout)
