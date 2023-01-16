import json
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build

class DataflowExecutor:
    def __init__(self, project_id, region='us-central1'):
        self.project_id = project_id
        self.region = region
        self.dataflow = build('dataflow', 'v1b3', cache_discovery=False)

    def run_template(self, template_path, options_path, job_name):
        """Run a Dataflow template"""
        with open(options_path, 'r') as f:
            options = json.load(f)

        request = self.dataflow.projects().locations().templates().launch(
            projectId=self.project_id,
            location=self.region,
            body={
                'jobName': job_name,
                'parameters': options,
                'gcsPath': template_path
            }
        )

        try:
            response = request.execute()
            print(f'Dataflow job {job_name} started with ID {response["job"]["id"]}')
        except HttpError as err:
            print(f'An error occurred: {err}')
            
    # Additional functions:
    def get_job_status(self, job_id):
        """Get the status of a Dataflow job"""
        request = self.dataflow.projects().locations().jobs().get(
            projectId=self.project_id,
            location=self.region,
            jobId=job_id
        )
        try:
            response = request.execute()
            print(f'Job {job_id} status: {response["currentState"]}')
        except HttpError as err:
            print(f'An error occurred: {err}')

    def cancel_job(self, job_id):
        """Cancel a Dataflow job"""
        request = self.dataflow.projects().locations().jobs().update(
            projectId=self.project_id,
            location=self.region,
            jobId=job_id,
            body={'requestedState': 'JOB_STATE_CANCELLED'}
        )
        try:
            response = request.execute()
            print(f'Job {job_id} was cancelled.')
        except HttpError as err:
            print(f'An error occurred: {err}')