from setuptools import setup, find_packages

setup(
    name='my_dataflow_job',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'apache-beam[gcp]==2.40.0',
        'apache-airflow==2.2.3',
    ],
    entry_points={
        'console_scripts': [
            'my_dataflow_job = my_dataflow_job.main:run',
        ],
    },
)
