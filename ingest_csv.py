import logging
import argparse

import csv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import ParDo
from apache_beam import Pipeline

from dependencies import convert_to_csv

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
    known_args, pipeline_args = parser.parse_known_args()
    return known_args, pipeline_args

class ToCsv(beam.DoFn):

    def process(self, element) :
        gen = convert_to_csv.read_the_csv(element)
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

    with Pipeline(options=pipeline_options) as pipeline:
        clean_csv = pipeline | 'Read input file' >> beam.io.textio.ReadFromText(
                in_path, skip_header_lines = 2, ) \
        | 'Convert CSV to list' >> ParDo(ToCsv()) \
        | 'print1' >> beam.Map(print)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    run(
    )
