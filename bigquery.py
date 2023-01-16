import os
from google.cloud import bigquery

class Bigquery:
    def __init__(self, project_id):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/credentials.json"
        self.client = bigquery.Client(project=project_id)

    def create_dataset(self, dataset_id):
        """Create a new BigQuery dataset"""
        dataset = bigquery.Dataset(f'{self.client.project}.{dataset_id}')
        dataset.create()
        print(f'Dataset {dataset_id} created.')

    def create_table(self, dataset_id, table_id, schema):
        """Create a new BigQuery table"""
        table = bigquery.Table(f'{self.client.project}.{dataset_id}.{table_id}', schema=schema)
        table.create()
        print(f'Table {table_id} created.')

    def load_data(self, dataset_id, table_id, file_path):
        """Load data into a BigQuery table"""
        table_ref = self.client.dataset(dataset_id).table(table_id)
        with open(file_path, 'rb') as source_file:
            job = self.client.load_table_from_file(
                source_file,
                table_ref,
                location='US'
            )
        job.result()
        print(f'Data loaded into table {table_id}.')

    def query_data(self, query):
        """Run a query on a BigQuery table"""
        query_job = self.client.query(query)
        results = query_job.result()
        return results
    
# Additional functions:

    def update_table(self, dataset_id, table_id, schema):
        """Update an existing BigQuery table schema"""
        table = self.client.get_table(f'{self.client.project}.{dataset_id}.{table_id}')
        table.schema = schema
        table = self.client.update_table(table, ["schema"])  # API request
        print(f'Table {table_id} schema updated.')

    def delete_table(self, dataset_id, table_id):
        """Delete a BigQuery table"""
        table_ref = self.client.dataset(dataset_id).table(table_id)
        self.client.delete_table(table_ref)
        print(f'Table {table_id} deleted.')

    def list_tables(self, dataset_id):
        """List all tables in a BigQuery dataset"""
        dataset_ref = self.client.dataset(dataset_id)
        tables = list(self.client.list_tables(dataset_ref))
        print(f'Tables in {dataset_id} dataset:')
        for table in tables:
            print(table.table_id)

    def export_to_gcs(self, dataset_id, table_id, bucket_name, file_name):
        """Export a BigQuery table to a GCS bucket"""
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        destination_uri = f'gs://{bucket_name}/{file_name}'
        job = self.client.extract_table(table_ref, destination_uri)
        job.result()
        print(f'Table {table_id} exported to {destination_uri}.')


