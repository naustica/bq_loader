from PyInquirer import prompt
from bq_loader import create_table


questions = [
    {
        'type': 'input',
        'name': 'table_id',
        'message': 'Please enter a table id'
    },
    {
        'type': 'input',
        'name': 'file_path',
        'message': 'Please enter a file path'
    },
    {
        'type': 'input',
        'name': 'schema_file',
        'message': 'Please enter a schema file'
    },
    {
        'type': 'list',
        'name': 'source_format',
        'message': 'Please choose a source format',
        'choices': ['jsonl', 'avro', 'csv', 'mro', 'orc', 'parquet']
    },
    {
        'type': 'list',
        'name': 'write_disposition',
        'message': 'Please choose an action that should occur if the destination table already exists',
        'choices': ['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY']
    },
    {
        'type': 'input',
        'name': 'destination_table_description',
        'message': 'Please enter a description'
    },
    {
        'type': 'confirm',
        'name': 'ignore_unknown_values',
        'message': 'Ignore unknown values?',
    },
]

def main():
    answers = prompt(questions)
    create_table(table_id=answers['table_id'],
                 file_path=answers['file_path'],
                 schema_file=answers['schema_file'],
                 source_format=answers['source_format'],
                 write_disposition=answers['write_disposition'],
                 destination_table_description=answers['destination_table_description'],
                 ignore_unknown_values=answers['ignore_unknown_values'])

if __name__ == '__main__':
    main()
