from datastore import DataStore
from dataflow_executor import DataflowExecutor
from dataproc import DataProc
from dataprep import DataprepWrapper
from bigquery import Bigquery
from mlengine import MLEngine

def main():
    project_id = "my-project-id"
    bucket_name = "my-bucket"
    data_source1 = "gs://my-bucket/data1.csv"
    data_source2 = "gs://my-bucket/data2.csv"
    data_source3 = "gs://my-bucket/data3.csv"
    dp_dataset_id = "my-dataprep-dataset"
    dp_recipe_id = "my-dataprep-recipe"
    dataflow_template = "gs://my-bucket/dataflow-template.json"
    options_file = "gs://my-bucket/options.json"
    cluster_name = "my-dataproc-cluster"
    zone = "us-central1-a"
    job_name = "my-dataproc-job"
    job_file_gcs_uri = "gs://my-bucket/job.jar"
    job_id = "my-mlengine-job"
    training_data = "gs://my-bucket/training-data"
    output_dir = "gs://my-bucket/output"

    # Create a DataStore object
    ds = DataStore(project_id, bucket_name)
    # Load data into the bucket
    ds.load_data(data_source1, data_source2, data_source3)

    # Create a DataflowExecutor object
    df = DataflowExecutor(project_id)
    # Execute the dataflow template
    df.run_template(dataflow_template, options_file)

    # Create a DataProc object
    dp = DataProc(project_id)
    # Create a cluster
    dp.create_cluster(cluster_name, zone)
    # Submit a job to the cluster
    dp.submit_job(cluster_name, job_name, job_file_gcs_uri)

    # Create a DataprepWrapper object
    dpw = DataprepWrapper(project_id)
    # Run the recipe on the dataset
    dpw.run_recipe(dp_dataset_id, dp_recipe_id)

    # Create a Bigquery object
    bq = Bigquery(project_id)
    # Load data into Bigquery
    bq.load_data(dp_dataset_id, dp_recipe_id)

    # Create a MLEngine object
    ml = MLEngine(project_id)
    # Train a model on Cloud ML Engine
    ml.train_model(job_id, training_data, output_dir)
