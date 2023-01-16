import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

class DataProc:
    def __init__(self, project_id):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/credentials.json"
        self.project_id = project_id
        self.service = build('dataproc', 'v1')

    def create_cluster(self, cluster_name, zone):
        """Create a new DataProc cluster"""
        request = self.service.projects().regions().clusters().create(
            projectId=self.project_id,
            region='global',
            body={
                'clusterName': cluster_name,
                'config': {
                    'gceClusterConfig': {
                        'zoneUri': f'{zone}'
                    }
                }
            }
        )
        try:
            request.execute()
            print(f'Cluster {cluster_name} created.')
        except HttpError as err:
            print(f'An error occurred: {err}')

    def submit_job(self, cluster_name, job_name, job_file_gcs_uri):
        """Submit a job to the DataProc cluster"""
        request = self.service.projects().regions().jobs().submit(
            projectId=self.project_id,
            region='global',
            body={
                'job': {
                    'placement': {
                        'clusterName': cluster_name
                    },
                    'hadoopJob': {
                        'mainJarFileUri': job_file_gcs_uri
                    },
                    'jobName': job_name
                }
            }
        )
        try:
            request.execute()
            print(f'Job {job_name} submitted to cluster {cluster_name}.')
        except HttpError as err:
            print(f'An error occurred: {err}')
            
    #Additional functions:
    
    def get_job_status(self, job_id):
        """Get the status of a job"""
        request = self.service.projects().regions().jobs().get(
            projectId=self.project_id,
            region='global',
            jobId=job_id
        )
        try:
            response = request.execute()
            print(f'Job {job_id} is in state {response["status"]["state"]}')
        except HttpError as err:
            print(f'An error occurred: {err}')

    def cancel_job(self, job_id):
        """Cancel a job"""
        request = self.service.projects().regions().jobs().cancel(
            projectId=self.project_id,
            region='global',
            jobId=job_id
        )
        try:
            request.execute()
            print(f'Job {job_id} cancelled.')
        except HttpError as err:
            print(f'An error occurred: {err}')

    def delete_cluster(self, cluster_name):
        """Delete a DataProc cluster"""
        request = self.service.projects().regions().clusters().delete(
            projectId=self.project_id,
            region='global',
            clusterName=cluster_name
        )
        try:
            request.execute()
            print(f'Cluster {cluster_name} deleted.')
        except HttpError as err:
            print(f'An error occurred: {err}')