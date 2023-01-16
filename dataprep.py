import json
import requests
from googleapiclient.errors import HttpError

class DataprepWrapper:
    def __init__(self, token):
        self.token = token
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }

    def create_recipe(self, recipe_name, project_id, wrangled_dataset_id):
        """Create a new DataPrep recipe"""
        url = f'https://api.dataprep.com/v4/recipe'

        body = {
            'name': recipe_name,
            'projectId': project_id,
            'wrangledDataset': {
                'id': wrangled_dataset_id
            }
        }

        try:
            response = requests.post(url, headers=self.headers, json=body)
            response.raise_for_status()
            recipe_id = response.json()['id']
            print(f'Recipe {recipe_name} created with ID {recipe_id}.')
            return recipe_id
        except HttpError as err:
            print(f'An error occurred: {err}')

    def run_recipe(self, recipe_id, run_name):
        """Run a DataPrep recipe"""
        url = f'https://api.dataprep.com/v4/recipe/{recipe_id}/run'

        body = {
            'name': run_name
        }

        try:
            response = requests.post(url, headers=self.headers, json=body)
            response.raise_for_status()
            run_id = response.json()['id']
            print(f'Recipe run {run_name} started with ID {run_id}.')
            return run_id
        except HttpError as err:
            print(f'An error occurred: {err}')