import apache_beam as beam
import argparse
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery


bucket_path = 'gs://jasm_liga_soccer/liga_spain'
file_pattern = 'season_*'

def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = beam.options.pipeline_options.PipelineOptions(pipeline_args)
    p = beam.Pipeline(options=pipeline_options)
    (
        p
        | 'Read CSV Files' >> ReadFromText(file_pattern=bucket_path + '/' + file_pattern)
        | 'Transform Data' >> beam.Map(lambda line: line.split(','))
        | 'Write to BigQuery' >> WriteToBigQuery(
            table='liga_spain.raw_data',
            schema='SCHEMA_AUTODETECT',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        )
    )
    p.run().wait_until_finish()

if __name__ == '__main__':
    run()
