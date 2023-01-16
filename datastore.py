import io
import json
import os

from google.cloud import storage

class DataStore:
    def init(self, project_id, bucket_name):
        self.project_id = project_id
        self.client = storage.Client(project=project_id)
        self.bucket_name = bucket_name
        self.bucket = self.client.get_bucket(self.bucket_name)
        self.blob = None

    def create_bucket(self):
        """Create a new bucket if it doesn't exist yet"""
        if not self.client.get_bucket(self.bucket_name).exists():
            self.bucket = self.client.create_bucket(self.bucket_name)
            print(f'Bucket {self.bucket.name} created.')
        else:
            self.bucket = self.client.get_bucket(self.bucket_name)
            print(f'Bucket {self.bucket.name} already exists.')

    def upload_data(self, file_path):
        """Upload data to a bucket"""
        self.blob = self.bucket.blob(file_path)
        with open(file_path, 'rb') as f:
            self.blob.upload_from_file(f)
        print(f'File {file_path} uploaded to {self.bucket.name}.')

    def download_data(self, destination_path):
        """Download data from a bucket"""
        self.blob.download_to_filename(destination_path)
        print(f'File {self.blob.name} downloaded from {self.bucket.name} to {destination_path}.')

    def delete_data(self):
        """Delete data from a bucket"""
        self.blob.delete()
        print(f'File {self.blob.name} deleted from {self.bucket.name}.')
        
    # Additional Functionalities:
    def list_blobs(self):
        """List all blobs in a bucket"""
        blobs = self.bucket.list_blobs()
        for blob in blobs:
            print(blob.name)

    def check_blob_exists(self, file_path):
        """Check if a specific blob exists in a bucket"""
        blob = self.bucket.blob(file_path)
        if blob.exists():
            print(f'Blob {file_path} exists in {self.bucket.name}.')
        else:
            print(f'Blob {file_path} does not exist in {self.bucket.name}.')