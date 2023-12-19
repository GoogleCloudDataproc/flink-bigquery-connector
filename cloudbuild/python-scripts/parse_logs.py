"""Python Script to parse yarn logs to check of job success.

This python file extracts the status of a dataproc job from its job-id and then
uses this job-id to get the yarn application number.
The yarn application number enables it to read through the yarn logs to check
for the number iof records.
In case the number of records match that of BQ table it returns, in case of
mismatch, it throws an error.
"""

from collections.abc import Sequence

from absl import app
from google.cloud import bigquery
from google.cloud import dataproc_v1
from google.cloud import storage
import re


def get_bq_query_rows(client_project_name):
    client = bigquery.Client(project=client_project_name)
    query = 'SELECT count(*) as count FROM `testproject-398714.testing_dataset.largeTable` where EXTRACT(HOUR from ts) = 17 and EXTRACT(DAY from ts) = 17;'
    query_job = client.query(query)
    query_result = query_job.result()
    records_read = 0
    for result in query_result:
        records_read = result[0]
    return records_read

# Remember these are the ones from args.
def get_bq_table_rows(client_project_name, project_name, dataset_name, table_name, query):
    if query:
        return get_bq_query_rows(client_project_name)
    dataset_ref = bigquery.DatasetReference(project=project_name, dataset_id=dataset_name)
    table_ref = bigquery.TableReference(dataset_ref=dataset_ref, table_id=table_name)
    client = bigquery.Client(project=client_project_name)
    table = client.get_table(table=table_ref)
    row_count = table.num_rows
    return row_count

def extract_metric(logs_as_string, metric_string, delimiter):
    total_metric_sum_in_blob = 0
    # Keep on finding the metric value as there can be
    # 1 or more outputs in a log file.
    metric_pattern = r'{}\s*(.*?)\s*{}'.format(re.escape(metric_string), re.escape(delimiter))
    metric_pattern = re.compile(metric_pattern)
    matches = metric_pattern.finditer(logs_as_string)
    records_read = [int(record_read.group(1).strip()) for record_read in matches]
    total_metric_sum_in_blob += sum(records_read)
    return total_metric_sum_in_blob

def check_query_correctness(logs_as_string):
    query_records_pattern = r'\[\s(.*),\s(.*)\s\]'
    # Find all matches of the pattern in the string
    matches = re.findall(query_records_pattern, logs_as_string)
    # Extract and print all pairs of HOUR and DAY
    if matches:
        for match in matches:
            hour = match[0].strip()
            day = match[1].strip()
            if hour !='17' or day !='17':
                raise RuntimeError(f'[Log: parse_logs ERROR] Incorrect query result obtained!')
    else:
        # If no such matches are found.
        print(f'[Log: parse_logs INFO] No query result obtained in this file')
        return False
    return True

def get_blob_and_check_metric(
    gcs_log_object, cluster_temp_bucket, metric_string, metric_delimiter, query
):
    logs_as_string = ''
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(cluster_temp_bucket)
        blob = bucket.blob(gcs_log_object)
        logs_as_string = blob.download_as_text(encoding='latin-1')
    except Exception as e:
        print(f'[Log: parse_logs WARNING] File {gcs_log_object} Not Found.\nError: {e}')

    if query:
        # In case query has been set:
        # If for any of the record, HOUR and DAY are not 17 then throw an error.
        check_query_correctness(logs_as_string)
    if metric_string in logs_as_string:
        metric_value = extract_metric(logs_as_string, metric_string, metric_delimiter)
        return metric_value

    return -1


def get_logs_pattern(client_project_name, region, job_id):
    """ Method to return dataproc job state and yarn logs path.

  Args:
      client_project_name: Project ID of the GCP project that contains the cluster.
      region: region in which the cluster runs.
      job_id: JOB_Id of the dataproc job.

  Returns:
    The path pattern inside the temp bucket in which logs are found.

  Raises:
      RuntimeError: In case the Dataproc job fails
  """
    # Create a client
    client = dataproc_v1.JobControllerClient(
        client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'}
    )

    # Initialize request argument(s)
    request = dataproc_v1.GetJobRequest(
        project_id=client_project_name,
        region=region,
        job_id=job_id,
    )

    # Make the request
    response = client.get_job(request=request)
    state = response.status.state.name

    if state == 'ERROR':
        raise RuntimeError(f'[Log: parse_logs ERROR] Dataproc Job with JOB ID: "{job_id}" failed')

    # If the dataproc job did not fail, continue to match the number of records.
    cluster_id = response.placement.cluster_uuid
    tracking_url = response.yarn_applications[0].tracking_url
    pattern = r'/([^/]+)(?:/)?$'
    # /([^/]+): Searches for a '/' that may or may not be followed by '/' as a group
    # Optional '/' at the end.
    yarn_application_number = re.search(pattern, tracking_url).group(1)
    pattern = r'[^_]+$'
    yarn_job_number = re.search(pattern, yarn_application_number).group(0)

    logs_pattern = (f'{cluster_id}/yarn-logs/root'
                    f'/bucket-logs-ifile/{yarn_job_number}/{yarn_application_number}')
    return logs_pattern


def get_cluster_temp_bucket(client_project_name, cluster_name, region):
    cluster_client = dataproc_v1.ClusterControllerClient(
        client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'}
    )
    get_cluster_request = dataproc_v1.GetClusterRequest(project_id=client_project_name, region=region,
                                                        cluster_name=cluster_name)
    cluster_resource = cluster_client.get_cluster(get_cluster_request)
    return cluster_resource.config.temp_bucket


def get_bucket_contents(bucket):
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket)
    bucket_contents = []
    # Note: The call returns a response only when the iterator is consumed.
    for blob in blobs:
        bucket_contents.append(blob.name)
    return bucket_contents

def read_logs(cluster_temp_bucket, logs_pattern, query):
    """Method to parse the number of records from the yarn logs.

  Args:
    cluster_temp_bucket: name of the temp bucket that the dataproc cluster uses to store yarn logs.
    logs_pattern: pattern in which logs are found inside the cluster temp bucket.

  Returns:
      Sum of metric values in all the files.

  Raises:
      RuntimeError: In case metric value is not found in any of the files.
  """

    metric_string = 'Number of records read: '
    metric_delimiter = ';'

    # Sum across all the worker files.
    total_metric_count = 0
    # Check if metric is present in at least one of the files.
    is_metric_found = False
    # Get all the contents in the GCS Bucket.
    gcs_bucket_contents = get_bucket_contents(cluster_temp_bucket)
    # Form the logs_pattern
    logs_pattern = re.compile(fr'{re.escape(logs_pattern)}/.+')
    # Find all strings in the array that match the pattern
    gcs_log_objects = [gcs_log_object for gcs_log_object in gcs_bucket_contents if logs_pattern.match(gcs_log_object)]
    print("The matching logs are found at:")
    for gcs_log_object in gcs_log_objects:
        print(f'gs://{cluster_temp_bucket}/{gcs_log_object}')
        # -1 is returned in case metric not found in the log file.
        metric_value = get_blob_and_check_metric(
            gcs_log_object,cluster_temp_bucket, metric_string, metric_delimiter, query)
        if metric_value != -1:
            is_metric_found = True
            # Sum up all the values.
            total_metric_count += metric_value
    # If found in any of the logs, return the value, else raise an error.
    if is_metric_found:
        return total_metric_count
    raise RuntimeError(f'[Log: parse_logs ERROR] Unable to find the "{metric_string}" in any of the logs')


def run(
    cluster_project_name, cluster_name, region, job_id, arg_project, arg_dataset, arg_table, query
):
    """Method that calls all the helper function to determine success of a job.

    Args:
      cluster_project_name: Project ID of the GCP project that contains the cluster.
      cluster_name: name of the cluster on which the dataproc job is running.
      region: region in which the cluster runs.
      job_id: JOB_Id of the dataproc job.
      arg_project: Resource project id (from which rows are read)
      arg_dataset: Resource dataset name (from which rows are read)
      arg_table: Resource table name (from which rows are read)
      query: String containing the query incase needs to be run by the connector.

    Raises:
      AssertionError: When the rows read by connector and in the BQ table do not
      match.
    """
    # 1. Get the temp bucket name.
    cluster_temp_bucket = get_cluster_temp_bucket(cluster_project_name, cluster_name, region)
    # 2. get the pattern of logs inside the temp bucket in GCS.
    logs_pattern = get_logs_pattern(cluster_project_name, region, job_id)
    # Read the blob and get the metric from them
    metric = read_logs(cluster_temp_bucket, logs_pattern, query)
    bq_table_rows = get_bq_table_rows(cluster_project_name, arg_project, arg_dataset, arg_table, query)
    if metric != bq_table_rows:
        raise AssertionError('[Log: parse_logs ERROR] Rows do not match')


def validate_arguments(arguments_dictionary, required_arguments, acceptable_arguments):
    for required_argument in required_arguments:
        if required_argument not in arguments_dictionary:
            raise UserWarning(f'[Log: parse_logs ERROR] {required_argument} argument not provided')
    for key, _ in arguments_dictionary.items():
        if key not in acceptable_arguments:
            raise UserWarning(f'[Log: parse_logs ERROR] Invalid argument "{key}" provided')


def main(argv: Sequence[str]) -> None:
    acceptable_arguments = {'job_id', 'project_id', 'cluster_name', 'region',
                            'project_name', 'dataset_name', 'table_name', 'query'}
    required_arguments = acceptable_arguments - {'query'}

    if len(argv) > len(acceptable_arguments) + 1:
        raise app.UsageError('[Log: parse_logs ERROR] Too many command-line arguments.')
    elif len(argv) < len(required_arguments) + 1:
        raise app.UsageError('[Log: parse_logs ERROR] Too less command-line arguments.')

    argument_pattern = r'--(\w+)=(.*)'
    # Forming a dictionary from the arguments
    try:
        matches = [re.match(argument_pattern, argument) for argument in argv[1:]]
        arguments_dictionary = {match.group(1): match.group(2) for match in matches}
        del matches
    except AttributeError:
        raise UserWarning('[Log: parse_logs ERROR] Missing argument value. Please check the arguments provided again.')
    # Validating if all necessary arguments are available
    validate_arguments(arguments_dictionary, required_arguments, acceptable_arguments)
    # Providing the values.
    job_id = arguments_dictionary['job_id']
    project_id = arguments_dictionary['project_id']
    cluster_name = arguments_dictionary['cluster_name']
    region = arguments_dictionary['region']
    project_name = arguments_dictionary['project_name']
    dataset_name = arguments_dictionary['dataset_name']
    table_name = arguments_dictionary['table_name']
    query = ''
    if 'query' in arguments_dictionary:
        query = arguments_dictionary['query']

    run(
        project_id, cluster_name, region, job_id, project_name, dataset_name, table_name, query
    )


if __name__ == '__main__':
    app.run(main)
