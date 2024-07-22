import logging
import argparse

import csv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import ParDo
from apache_beam import Pipeline

from dependencies import convert_to_csv, to_dict

def _get_args()-> object:
    parser = argparse.ArgumentParser()
    parser.add_argument( '--temp_location', '-tl',
             required = True,
             help='temp location for GCP temp bucket ')
    parser.add_argument( '--runner', '-r',
            choices = ['DataflowRunner', 'DirectRunner'],
            default = 'DataflowRunner',
        help='runner; local testing uses DirectRunner; production uses GCP ')
    parser.add_argument( '--template_location', '-t',
            default = None,
        help='if creating a template')
    parser.add_argument( '--path', '-p',
            default = None,
            required = True,
        help='path to bucket or directory')
    parser.add_argument( '--table-name', 
            required = True,
        help='name of table')
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args

class ToCsv(beam.DoFn):

    def process(self, element) :
        gen = convert_to_csv.read_the_csv(element)
        for i in gen:
            yield i

class FilterFields(beam.DoFn):

    def process(self, element) :
        l = [element[2], element[72], element[44], element[82],
                element[42], element[43], element[6]]
        yield l

class ToDict(beam.DoFn):

    def __init__(self, schema):
        self.schema = to_dict.get_schema(schema)

    def process(self, element):
        gen = to_dict.to_the_dict(l = element, schema = self.schema)
        for i in gen:
            yield i


def run(pipeline_args:list=None):
    known_args, pipeline_args = _get_args()
    project = 'paul-henry-tremblay'
    pipeline_args = ['--region',  'us-central1',  '--project',project , '--temp_location',  
          f'gs://{known_args.temp_location}', '--runner', known_args.runner] 
    pipeline_options = PipelineOptions(
        pipeline_args, 
        streaming=False, 
        save_main_session=True,
        template_location= known_args.template_location,
    )
    in_path = f'{known_args.path}/*'
    table_schema = 'total_issued:INTEGER, current:INTEGER, late:FLOAT, charged:INTEGER, principal_payments_received:FLOAT, interest_payments_received:FLOAT, avg_interest:FLOAT' 

    with Pipeline(options=pipeline_options) as pipeline:
        clean_csv = pipeline | 'Read input file' >> beam.io.textio.ReadFromText(
                in_path, skip_header_lines = 2, ) \
        | 'Convert CSV to list' >> ParDo(ToCsv()) \
        | 'Filter fields' >> ParDo(FilterFields()) \
        | "to dict " >> ParDo(ToDict(schema = table_schema)) \
        | " To BQ" >> beam.io.WriteToBigQuery(
                        known_args.table_name,
                        schema=table_schema,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    run(
    )
