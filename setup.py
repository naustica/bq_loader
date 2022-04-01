from setuptools import setup
from os import path

dir = path.abspath(path.dirname(__file__))
with open(path.join(dir, 'README.md'), encoding='utf-8') as file:
    long_description = file.read()


setup(name='bq_loader',
      version='0.1.2',
      description='',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/naustica/bq_loader',
      author='Nick Haupka',
      author_email='nick.haupka@gmail.com',
      license='MIT',
      packages=['bq_loader'],
      keywords=['BigQuery'],
      project_urls={
        'Source': 'https://github.com/naustica/bq_loader',
        'Tracker': 'https://github.com/naustica/bq_loader/issues'
      },
      install_requires=[
        'google-cloud-bigquery',
        'google-cloud-storage',
        'google-api-core',
        'PyInquirer',
        'prompt_toolkit==1.0.14'
      ],
      extras_require={
       'dev': [
           'pytest',
           'coverage',
           'pytest-cov',
           'sphinx',
           'alabaster'
       ]
      },
      entry_points={
        'console_scripts': [
            'bqloader = bq_loader.__main__:main'
        ]
      },
      classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.9'
      ],
      zip_safe=False)
