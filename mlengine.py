from googleapiclient import errors
from googleapiclient.discovery import build

class MLEngine:
    def __init__(self, project_id):
        self.project_id = project_id
        self.service = build('ml', 'v1')

    def train_model(self, job_id, training_data, output_dir):
        """Train a model on Cloud ML Engine"""
        try:
            request = self.service.projects().jobs().create(
                parent=f'projects/{self.project_id}',
                body={
                    'jobId': job_id,
                    'trainingInput': {
                        'scaleTier': 'STANDARD_1',
                        'inputDataConfig': [{
                            'name': 'training_data',
                            'uri': training_data
                        }],
                        'outputDataConfig': {
                            'outputPath': output_dir
                        }
                    }
                }
            )
            request.execute()
            print(f'Model training job {job_id} started.')
        except errors.HttpError as err:
            print(f'An error occurred: {err}')
            
    # Additional functions:
    
    def deploy_model(self, model_name, version_name, runtime_version):
        """Deploy the trained model on Cloud ML Engine"""
        try:
            request = self.service.projects().models().create(
                parent=f'projects/{self.project_id}',
                body={
                    'name': model_name,
                    'deployment_metadata': {
                        'runtime_version': runtime_version
                    }
                }
            )
            request.execute()
            request = self.service.projects().models().versions().create(
                parent=f'projects/{self.project_id}/models/{model_name}',
                body={
                    'name': version_name
                }
            )
            request.execute()
            print(f'Model {model_name} deployed with version {version_name}.')
        except errors.HttpError as err:
            print(f'An error occurred: {err}')
    
    def list_models(self):
        """List all models on Cloud ML Engine"""
        try:
            request = self.service.projects().models().list(
                parent=f'projects/{self.project_id}'
            )
            response = request.execute()
            if 'models' in response:
                models = response['models']
                for model in models:
                    print(f'Model name: {model["name"]}')
            else:
                print('No models found.')
        except errors.HttpError as err:
            print(f'An error occurred: {err}')
            
    def delete_model(self, model_name):
        """Delete a model on Cloud ML Engine"""
        try:
            request = self.service.projects().models().delete(
                name=f'projects/{self.project_id}/models/{model_name}'
            )
            request.execute()
            print(f'Model {model_name} deleted.')
        except errors.HttpError as err:
            print(f'An error occurred: {err}')

    def create_version(self, model_name, version_name, runtime_version, python_version):
        """Create a new version of a model on Cloud ML Engine"""
        try:
            request = self.service.projects().models().versions().create(
                parent=f'projects/{self.project_id}/models/{model_name}',
                body={
                    'name': version_name,
                    'runtimeVersion': runtime_version,
                    'pythonVersion': python_version
                }
            )
            request.execute()
            print(f'Version {version_name} created for model {model_name}.')
        except errors.HttpError as err:
            print(f'An error occurred: {err}')
            
    def set_default_version(self, model_name, version_name):
        """Set a version of a model as the default on Cloud ML Engine"""
        try:
            request = self.service.projects().models().versions().setDefault(
                name=f'projects/{self.project_id}/models/{model_name}/versions/{version_name}'
            )
            request.execute()
            print(f'Version {version_name} set as default for model {model_name}.')
        except errors.HttpError as err:
            print(f'An error occurred: {err}')
            
            
    
    
