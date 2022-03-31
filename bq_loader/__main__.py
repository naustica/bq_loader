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
        'message': 'Please enter a source format',
        'choices': ['jsonl', 'avro', 'csv', 'mro', 'orc', 'parquet']
    },
    {
        'type': 'confirm',
        'name': 'ignore_unknown_values',
        'message': 'Should be unknown values ignored?',
    },
]

def main():
    answers = prompt(questions)
    create_table(table_id=answers['table_id'],
                 file_path=answers['file_path'],
                 schema_file=answers['schema_file'],
                 source_format=answers['source_format'],
                 ignore_unknown_values=answers['ignore_unknown_values'])

if __name__ == '__main__':
    main()
