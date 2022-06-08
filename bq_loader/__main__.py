import inquirer
from bq_loader import create_table_from_local, create_table_from_bucket, upload_files_to_bucket


introduction_question = [
    inquirer.List('method',
                  message='Please choose a method',
                  choices=['create_table_from_local',
                              'create_table_from_bucket',
                              'upload_files_to_bucket'])
]

questions_create_table_from_local = [
    inquirer.Text('table_id',
                  message='Please enter a table id'),

    inquirer.Text('project_id',
                  message='Please enter a project id'),

    inquirer.Text('dataset_id',
                  message='Please enter a dataset id'),

    inquirer.Text('file_path',
                  message='Please enter a file path'),

    inquirer.Text('schema_file_path',
                  message='Please enter a schema file'),

    inquirer.List('source_format',
                  message='Please choose a source format',
                  choices=['jsonl', 'avro', 'csv', 'mro', 'orc', 'parquet']),

    inquirer.List('write_disposition',
                  message='Please choose an action that should occur if the destination table already exists',
                  choices=['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY']),

    inquirer.Text('table_description',
                  message='Please enter a table description'),

    inquirer.Confirm('ignore_unknown_values',
                     message='Ignore unknown values?')
]

questions_create_table_from_bucket = [
    inquirer.Text('uri',
                  message='Please enter a Google Bucket URI'),

    inquirer.Text('table_id',
                  message='Please enter a table id'),

    inquirer.Text('project_id',
                  message='Please enter a project id'),

    inquirer.Text('dataset_id',
                  message='Please enter a dataset id'),

    inquirer.Text('schema_file_path',
                  message='Please enter a schema file'),

    inquirer.List('source_format',
                  message='Please choose a source format',
                  choices=['jsonl', 'avro', 'csv', 'mro', 'orc', 'parquet']),

    inquirer.List('write_disposition',
                  message='Please choose an action that should occur if the destination table already exists',
                  choices=['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY']),

    inquirer.Text('table_description',
                  message='Please enter a table description'),

    inquirer.Confirm('ignore_unknown_values',
                  message='Ignore unknown values?')
]

questions_upload_files_to_bucket = [
    inquirer.Text('bucket_name',
                  message='Please enter a bucket name'),

    inquirer.Text('file_path',
                  message='Please enter a file path'),

    inquirer.Text('gcb_dir',
                  message='Please enter the name of the directory which should be created')
]

def main():

    answer = inquirer.prompt(introduction_question)

    if answer['method'] == 'create_table_from_local':

        answers = inquirer.prompt(questions_create_table_from_local)
        create_table_from_local(table_id=answers['table_id'],
                                project_id=answers['project_id'],
                                dataset_id=answers['dataset_id'],
                                file_path=answers['file_path'],
                                schema_file_path=answers['schema_file_path'],
                                source_format=answers['source_format'],
                                write_disposition=answers['write_disposition'],
                                table_description=answers['table_description'],
                                ignore_unknown_values=answers['ignore_unknown_values'])

    if answer['method'] == 'create_table_from_bucket':
        answers = inquirer.prompt(questions_create_table_from_bucket)
        create_table_from_bucket(uri=answers['uri'],
                                 table_id=answers['table_id'],
                                 project_id=answers['project_id'],
                                 dataset_id=answers['dataset_id'],
                                 schema_file_path=answers['schema_file_path'],
                                 source_format=answers['source_format'],
                                 write_disposition=answers['write_disposition'],
                                 table_description=answers['table_description'],
                                 ignore_unknown_values=answers['ignore_unknown_values'])

    if answer['method'] == 'upload_files_to_bucket':
        answers = inquirer.prompt(questions_upload_files_to_bucket)
        upload_files_to_bucket(bucket_name=answers['bucket_name'],
                               file_path=answers['file_path'],
                               gcb_dir=answers['gcb_dir'])

if __name__ == '__main__':
    main()
