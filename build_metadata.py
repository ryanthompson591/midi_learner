# read all the metadata out of a midi file and populates it into a database


import argparse

import mido

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery

import constants
from typing import Iterable

IS_TEST_ENVIRONMENT = True

SAMPLE_ONE_ELEMENT = True


def get_pipeline_options():
    parser = argparse.ArgumentParser()
    args, beam_args = parser.parse_known_args()

    if not IS_TEST_ENVIRONMENT:
        return PipelineOptions(
            beam_args,
            runner='DataflowRunner',
            project='midi-generator-fun-project',
            job_name='unique-job-name',
            temp_location='gs://runner_tmp',
            region='us-east1')
    else:
        return PipelineOptions(
            beam_args,
            runner='DirectRunner',
            project = 'midi-generator-fun-project',
            temp_location = 'gs://runner_tmp',
            region='us-east1',
        )



def transform_to_url(manifest_row) -> Iterable[str]:
  for row in manifest_row:
    yield 'gs://midi_data/' + row['url']


def _get_metadata(midi_file_url):
  mido_file = mido.MidiFile(file=midi_file_url)
  metadata = []
  for i, track in enumerate(mido_file.tracks):
    print('Track {}: {}'.format(i, track.name))
    track_data = {
      'track_name': [],
      'number': [],
      'copyright': [],
      'instrument_name': [],
      'text': [],
      'key_signature': [],
      'time_signature': []
    }
    metadata.append(track_data)
    for msg in track:
      if isinstance(msg, mido.midifiles.meta.MetaMessage):
        if msg.type == 'track_name':
          track_data['track_name'].append(msg.name)
        elif msg.type == 'number':
          track_data['number'].append(msg.value)
        elif msg.type == 'copyright':
          track_data['copyright'].append(msg.text)
        elif msg.type == 'instrument_name':
          track_data['instrument_name'].append(msg.name)
        elif msg.type == 'text':
          track_data['text'].append(msg.text)
        elif msg.type == 'key_signature':
          track_data['key_signature'].append(msg.key)
        elif msg.type == 'time_signature':
          track_data['time_signature'].append((msg.numerator, msg.denominator, msg.notated_32nd_notes_per_beat))
    return metadata


@beam.typehints.with_input_types(str)
class DumpMetadata(beam.DoFn):
  def process(self, file_url:str):
    print('opening file ' + file_url)
    with beam.io.gcsio.GcsIO().open(file_url, mode='rb') as file:
      meta_data = _get_metadata(file)
      return meta_data

def main():
  table_spec = bigquery.TableReference(
    projectId=constants.PROJECT_ID,
    datasetId=constants.INPUT_DATASET,
    tableId=constants.MANIFEST_TABLE)

  # Todo replace with data
  with beam.Pipeline(options=get_pipeline_options()) as pipeline:
      file_names = pipeline | beam.io.ReadFromBigQuery(table=table_spec)
      sampled_file_names =file_names
      if SAMPLE_ONE_ELEMENT:
        sampled_file_names = file_names | beam.combiners.Sample.FixedSizeGlobally(1)
      file_urls = sampled_file_names | 'mapped to url ' >> beam.FlatMap(transform_to_url)
      metadata = file_urls | beam.ParDo(DumpMetadata())
      metadata | beam.Map(print)




if __name__ == "__main__":
  main()