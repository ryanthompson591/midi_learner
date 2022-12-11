from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import constants

def create_table(dataset_id, table_name, schema=None):
  client = bigquery.Client(project=constants.PROJECT_ID)

  dataset = client.get_dataset(dataset_id)

  try:
    client.get_table('{}.{}.{}'.format(constants.PROJECT_ID,dataset_id,table_name))  # Make an API request.
    print("Table {} already exists.".format(table_name))
    return
  except NotFound:
    print("Table {} is not found.".format(table_name))

  table_ref = dataset.table(table_name)
  table = bigquery.Table(table_ref, schema=schema)

  # TODO: encryption not set --- use bigquery.EncryptionConfiguration
  table = client.create_table(table)  # API request


def print_schema(dataset_id, table_name):
  client = bigquery.Client(project=constants.PROJECT_ID)
  table = client.get_table('{}.{}.{}'.format(constants.PROJECT_ID, dataset_id, table_name))

  original_schema = table.schema
  print ('got schema of length {}'.format(len(original_schema)))
  names = set([field.name for field in original_schema])
  print(names)
