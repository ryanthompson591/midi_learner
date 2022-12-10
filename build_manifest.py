from google.cloud import storage
from google.cloud import bigquery

from typing import List

import constants
import common

DATA_SET = 'input_data'
MANIFEST_TABLE = 'manifest'

def list_blobs(bucket_name:str) -> List[str]:
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client(project=constants.PROJECT_ID)

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    # Note: The call returns a response only when the iterator is consumed.
    return [blob.name for blob in blobs]

def get_all_datasets(client) -> List[str]:
    datasets = list(client.list_datasets())  # Make an API request.
    return [dataset.dataset_id for dataset in datasets]


def create_data_set(dataset_id:str):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=constants.PROJECT_ID)

    if dataset_id in get_all_datasets(client):
      print ('dataset {} already exists.'.format(dataset_id))
      return

    dataset_id = "{}.{}".format(client.project, dataset_id)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)

    # TODO(developer): Specify the geographic location where the dataset should reside.
    dataset.location = constants.REGION

    # Send the dataset to the API for creation, with an explicit timeout.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))


def update_schema(dataset_id, table_name):
  client = bigquery.Client(project=constants.PROJECT_ID)
  table = client.get_table('{}.{}.{}'.format(constants.PROJECT_ID, dataset_id, table_name))

  original_schema = table.schema

  new_schema = original_schema[:]  # Creates a copy of the schema.
  names = set([field.name for field in original_schema])

  if 'url' not in names:
    new_schema.append(bigquery.SchemaField("url", "STRING"))
  if 'processed' not in names:
    new_schema.append(bigquery.SchemaField('processed', 'boolean'))

  table.schema = new_schema
  table = client.update_table(table, ["schema"])  # Make an API request.


def populate_manifest():
  client = bigquery.Client(project=constants.PROJECT_ID)
  table = client.get_table('{}.{}.{}'.format(constants.PROJECT_ID, DATA_SET, MANIFEST_TABLE))
  all_files = list_blobs('midi_data')
  print('adding all files total files - {}'.format(len(all_files)))

  rows = [{u'url': filename, u'processed': False} for filename in all_files]
  client.insert_rows(table, rows)

def create_and_populate_manifest()
  create_data_set('input_data')
  common.create_table('input_data', 'manifest')
  print_schema('input_data', 'manifest')
  update_schema('input_data', 'manifest')
  populate_manifest()

def main():
  #create_and_populate_manifest()
  pass

if __name__ == "__main__":
  main()