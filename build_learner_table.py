
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

parser = argparse.ArgumentParser()
args, beam_args = parser.parse_known_args()


beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    project='midi-generator-fun-project',
    job_name='unique-job-name',
    temp_location='gs://runner_temp',
    region='us-central1')


with beam.Pipeline(options=beam_options) as pipeline:
  pass # build pipeline here.