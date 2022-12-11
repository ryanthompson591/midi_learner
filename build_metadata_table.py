#sets up the database for metadata
from google.cloud import bigquery

import common
import constants

PROJECT_ID = 'midi-generator-fun-project'
DATA_SET = 'input_data'

# see https://mido.readthedocs.io/en/latest/meta_message_types.html


def main():
  schema = [
    bigquery.SchemaField("file_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("track", "RECORD", mode="REPEATED",
                         fields=[
                           bigquery.SchemaField("number", "INTEGER", mode="REPEATED"),
                           bigquery.SchemaField("copyright", "STRING", mode="REPEATED"),
                           bigquery.SchemaField("text", "STRING", mode="REPEATED"),
                           bigquery.SchemaField("track_name", "STRING", mode="REPEATED"),
                           bigquery.SchemaField("instrument_name", "STRING", mode="REPEATED"),
                           bigquery.SchemaField("key_signature", "STRING", mode="REPEATED"),
                           bigquery.SchemaField("time_signature", "RECORD", mode="REPEATED",
                                                fields=[
                                                  bigquery.SchemaField("numerator", "INTEGER", mode="REPEATED"),
                                                  bigquery.SchemaField("denominator", "INTEGER", mode="REPEATED"),
                                                  bigquery.SchemaField("clocks_per_tick", "INTEGER", mode="REPEATED"),
                                                  bigquery.SchemaField("notated_32nd_notes_per_beat", "INTEGER",
                                                                       mode="REPEATED"),
                                                ]
                                                ),
                         ]
                         )
  ]
  common.create_table('input_data', 'metadata', schema=schema)


if __name__=='__main__':
  main()