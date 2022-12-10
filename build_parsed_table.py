
import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


IS_TEST_ENVIRONMENT = True

def get_pipeline_options():
    parser = argparse.ArgumentParser()
    args, beam_args = parser.parse_known_args()

    if not IS_TEST_ENVIRONMENT:
        return PipelineOptions(
            beam_args,
            runner='DataflowRunner',
            project='midi-generator-fun-project',
            job_name='unique-job-name',
            temp_location='gs://runner_temp',
            region='us-east1')
    else:
        return PipelineOptions(
            beam_args,
            runner='DirectRunner'
        )


def main():

    # Todo replace with data
    with beam.Pipeline(options=get_pipeline_options()) as pipeline:
        file_names = pipeline | beam.Create(['test_data/albeniz_alb_esp1.mid'])
        fake_data | beam.Map(print)



if __name__ == "__main__":
  main()