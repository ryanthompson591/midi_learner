from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from typing import List

import constants

PROJECT_ID = 'midi-generator-fun-project'
DATA_SET = 'input_data'
MANIFEST_TABLE = 'manifest'

def list_blobs(bucket_name:str) -> List[str]:
    """Lists all the blobs in the bucket."""
    storage_client = storage.Client(project=PROJECT_ID)

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    # Note: The call returns a response only when the iterator is consumed.
    return [blob.name for blob in blobs]

def get_all_datasets(client) -> List[str]:
    datasets = list(client.list_datasets())  # Make an API request.
    return [dataset.dataset_id for dataset in datasets]


def create_data_set(dataset_id:str):
    # Construct a BigQuery client object.
    client = bigquery.Client(project=PROJECT_ID)

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


def create_table(dataset_id, table_name):
  client = bigquery.Client(project=PROJECT_ID)

  dataset = client.get_dataset(dataset_id)

  try:
    client.get_table('{}.{}.{}'.format(PROJECT_ID,dataset_id,table_name))  # Make an API request.
    print("Table {} already exists.".format(table_name))
    return
  except NotFound:
    print("Table {} is not found.".format(table_name))

  table_ref = dataset.table(table_name)
  table = bigquery.Table(table_ref)

  # TODO: encryption not set --- use bigquery.EncryptionConfiguration
  table = client.create_table(table)  # API request

def print_schema(dataset_id, table_name):
  client = bigquery.Client(project=PROJECT_ID)
  table = client.get_table('{}.{}.{}'.format(PROJECT_ID, dataset_id, table_name))

  original_schema = table.schema
  print ('got schema of length {}'.format(len(original_schema)))
  names = set([field.name for field in original_schema])
  print(names)

def update_schema(dataset_id, table_name):
  client = bigquery.Client(project=PROJECT_ID)
  table = client.get_table('{}.{}.{}'.format(PROJECT_ID, dataset_id, table_name))

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
  client = bigquery.Client(project=PROJECT_ID)
  table = client.get_table('{}.{}.{}'.format(PROJECT_ID, DATA_SET, MANIFEST_TABLE))
  all_files = list_blobs('midi_data')
  print('adding all files total files - {}'.format(len(all_files)))

  rows = [{u'url': filename, u'processed': False} for filename in all_files]
  client.insert_rows(table, rows)

def create_and_populate_manifest()
  create_data_set('input_data')
  create_table('input_data', 'manifest')
  print_schema('input_data', 'manifest')
  update_schema('input_data', 'manifest')
  populate_manifest()

def main():
  #create_and_populate_manifest()
  pass

if __name__ == "__main__":
  main()