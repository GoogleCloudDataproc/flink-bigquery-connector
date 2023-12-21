# Copyright 2022 Google Inc. All Rights Reserved.

"""Python Script to parse yarn logs to check of job success.

This python file extracts the status of a dataproc job from its job-id and then
uses this job-id to get the yarn application number.
The yarn application number enables it to read through the yarn logs to check
for the number iof records.
In case the number of records match that of BQ table it returns, in case of
mismatch, it throws an error.
"""

from collections.abc import Sequence
import re

from absl import app
from google.cloud import bigquery
from google.cloud import dataproc_v1
from google.cloud import storage


def get_bq_query_result_row_count(client_project_name, query):
    client = bigquery.Client(project=client_project_name)
    query_job = client.query(query)
    query_result = query_job.result()
    records_read = query_result.total_rows
    return records_read


def get_bq_table_row_count(
    client_project_name, project_name, dataset_name, table_name, query
):
    """Method to get count of rows in a BigQuery Table.

    Args:
      client_project_name: Name of the project to form the BQ Client
      project_name: Project ID that contains the table
      dataset_name: Name of Dataset containing the table
      table_name: Table Name
      query: Query string [if any] that was provided to the connector. Incase
        non-empty, The method executes this query and returns the number of rows
        obtained.

    Returns:
      Count of rows in the provided `project_name.dataset_name.table_name` table
    """
    if query:
        return get_bq_query_result_row_count(client_project_name, query)
    dataset_ref = bigquery.DatasetReference(
        project=project_name, dataset_id=dataset_name
    )
    table_ref = bigquery.TableReference(
        dataset_ref=dataset_ref, table_id=table_name
    )
    client = bigquery.Client(project=client_project_name)
    table = client.get_table(table=table_ref)
    row_count = table.num_rows
    return row_count


def extract_metric(logs_as_string, metric_string, end_of_metric_string):
    """Method to extract the metric value from logs downloaded as a string.

    Args:
      logs_as_string: Yarn application logs downloaded as a string.
      metric_string: The string to be found
      end_of_metric_string: Delimiter at the end of the value.

    Returns:
      Sum of values of the metric obtained in a file. As there can be 0 or more
      occurrences of metric in a file
    """
    total_metric_sum_in_blob = 0
    # Keep on finding the metric value as there can be
    # 1 or more outputs in a log file.

    # The logs are of the format
    # Number of records read: <value> ;
    # Here, "Number of records read: " is the metric string
    # and ";" is the end_of_metric_string.
    # We find the smallest possible string that matches.

    metric_pattern = r'{}\s*(.*?)\s*{}'.format(
        re.escape(metric_string), re.escape(end_of_metric_string)
    )
    metric_pattern = re.compile(metric_pattern)
    matches = metric_pattern.finditer(logs_as_string)
    records_read = [int(record_read.group(1).strip()) for record_read in matches]
    total_metric_sum_in_blob += sum(records_read)
    return total_metric_sum_in_blob


def check_query_correctness(gcs_log_object, logs_as_string):
    """Checks the correctness of query results obtained in the logs.

    This is a hardcoded check, done by checking if the records obtained
    from a "filter" query contains only the desired (filtered) values.
    Args:
      gcs_log_object: GCS log object (name of the GCS Object containing the logs)
      logs_as_string: Yarn application logs downloaded as a string.

    Returns:
      True if query result is seen in the file and does not violate filter
      condition.
      False in case no query result is obtained in the file (file as a string).

    Raises:
      RuntimeError: When filter condition is not met.
    """

    # Query records are of the format [ HOUR, DAY ]
    # The pattern searches for records formatted the same way.
    # A single space '\s' followed by a group (the HOUR), a ', ' and a space '\s'
    # which is again followed by a group (the DAY)

    query_records_pattern = r'\[\s(.*?),\s(.*?)\s\]'
    # Find all matches of the pattern in the string
    matches = re.findall(query_records_pattern, logs_as_string)
    # Extract and print all pairs of HOUR and DAY
    if matches:
        print(f'[Log: parse_logs INFO] Query result obtained in {gcs_log_object}')
        for match in matches:
            hour = match[0].strip()
            day = match[1].strip()

            # Check if the records thus obtained follow the filter condition.
            # Hardcoded check if HOUR and DAY are both = '17'
            if hour != '17' or day != '17':
                raise RuntimeError(
                    '[Log: parse_logs ERROR] Incorrect query result obtained!'
                )
    else:
        # If no such matches are found.
        print(
            '[Log: parse_logs WARNING] No query result obtained in'
            f' {gcs_log_object}'
        )
        return False
    return True


def get_blob_and_check_metric(
    gcs_log_object,
    cluster_temp_bucket,
    metric_string,
    end_of_metric_string,
    query,
):
    """Method to extract the yarn logs file as a string and find metric string.

    Args:
      gcs_log_object: string
      cluster_temp_bucket: name of the bucket (cluster temp bucket)
      metric_string: The string to be found
      end_of_metric_string: Delimiter at the end of the value.
      query: Query string [if any] that was provided to the connector.

    Returns:
    Tuple [Metric Value (String), Is Query Result Present (Boolean)]
      Metric value: which is the sum of metric values obtained from a file.
       -1 in case metric is not seen in the file.
      Is Query Result Present: Boolean value. True incase query result
        is present in the file, False if not.
    """
    # Obtain the yarn logs as a string from the GCS bucket.
    logs_as_string = ''
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(cluster_temp_bucket)
        blob = bucket.blob(gcs_log_object)
        logs_as_string = blob.download_as_text(encoding='latin-1')
    except Exception as e:
        print(
            f'[Log: parse_logs WARNING] File {gcs_log_object} Not'
            f' Found.\nError: {e}'
        )

    is_query_result_present = False
    if query:
        # In case query has been set:
        # If for any of the record, filter condition is not met then throw an error.
        is_query_result_present = check_query_correctness(
            gcs_log_object, logs_as_string
        )

    if metric_string in logs_as_string:
        # If metric string is present in the logs, extract the value.
        metric_value = extract_metric(
            logs_as_string, metric_string, end_of_metric_string
        )
        return metric_value, is_query_result_present
    # If not return -1
    return -1, is_query_result_present


def get_logs_pattern(client_project_name, region, job_id):
    """Method to return dataproc job state and yarn logs path.

    Args:
        client_project_name: Project ID of the GCP project that contains the
          cluster.
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
        raise RuntimeError(
            f'[Log: parse_logs ERROR] Dataproc Job with JOB ID: "{job_id}" failed'
        )

    # If the dataproc job did not fail, continue to match the number of records.
    cluster_id = response.placement.cluster_uuid
    tracking_url = response.yarn_applications[0].tracking_url

    # Tracking url is of the form http://.../proxy/application_..._.../
    # We need to extract the application number which is at the end of this url.
    # The pattern
    #     /([^/]+): Searches for a '/'. This is followed by set of characters
    #     which are not a '/', as a group.
    #     Optional '/' at the end (as some urls lack the ending '/')
    # Thus the current pattern helps in searching 'application_YYYYYY_XXXX'
    # as the part of group 1.

    tracking_url_pattern = r'/([^/]+)(?:/)?$'
    yarn_application_number = re.search(tracking_url_pattern, tracking_url).group(
        1
    )
    # With the extracted application number of the format
    # 'application_YYYYYY_XXXX'
    # We need to extract the actual application number within the cluster,
    # i.e. the ending digits which is the job number in the cluster.
    # The pattern
    #     [^_]+: Searches for the last occurrence of characters
    #     which are not a "_".
    #     i.e. the job number present after the last "_"
    # Thus the current pattern helps in searching "XXXX" as the part of group 0.

    yarn_application_number_pattern = r'[^_]+$'

    yarn_job_number = re.search(
        yarn_application_number_pattern, yarn_application_number
    ).group(0)

    logs_pattern = (
        f'{cluster_id}/yarn-logs/root'
        f'/bucket-logs-ifile/{yarn_job_number}/{yarn_application_number}'
    )
    return logs_pattern


def get_cluster_temp_bucket(client_project_name, cluster_name, region):
    cluster_client = dataproc_v1.ClusterControllerClient(
        client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'}
    )
    get_cluster_request = dataproc_v1.GetClusterRequest(
        project_id=client_project_name, region=region, cluster_name=cluster_name
    )
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
        cluster_temp_bucket: name of the temp bucket that the dataproc cluster
          uses to store yarn logs.
        logs_pattern: pattern in which logs are found inside the cluster temp
          bucket.
        query: query set by the user to be executed before the BQ table read.

    Returns:
        Sum of metric values in all the files.

    Raises:
        RuntimeError: In case metric value is not found in any of the files.
        Incase query has been set and query results were not found in any of the
        log files.
    """

    metric_string = 'Number of records read: '
    end_of_metric_string = ';'

    # Sum across all the worker files.
    total_metric_count = 0
    # Check if metric is present in at least one of the files.
    is_metric_found = False
    is_query_result_found = False
    # Get all the contents in the GCS Bucket.
    gcs_bucket_contents = get_bucket_contents(cluster_temp_bucket)
    # Form the logs_pattern

    # logs are stored in files having names of the format 'log_pattern/...'
    # the pattern enables searching for the same.

    logs_pattern = re.compile(rf'{re.escape(logs_pattern)}/.+')
    # Find all strings in the array that match the pattern
    gcs_log_objects = [
        gcs_log_object
        for gcs_log_object in gcs_bucket_contents
        if logs_pattern.match(gcs_log_object)
    ]
    for gcs_log_object in gcs_log_objects:
        # -1 is returned in case metric not found in the log file.
        (metric_value, is_query_result_present) = get_blob_and_check_metric(
            gcs_log_object,
            cluster_temp_bucket,
            metric_string,
            end_of_metric_string,
            query,
        )
        if metric_value != -1:
            is_metric_found = True
            # Sum up all the values.
            total_metric_count += metric_value

        # Check if query result is found in any one of the files.
        # True if found in at least one of the files.
        # False in case not found in any.
        if is_query_result_present:
            is_query_result_found = True

    # If query has been set, check if query results were obtained any
    # at least one of the logs. If not raise an Exception.
    if query and not is_query_result_found:
        raise RuntimeError(
            '[Log: parse_logs ERROR] Unable to find the query results in any of the'
            ' logs'
        )
    # If found in any of the logs, return the value, else raise an error.
    if is_metric_found:
        return total_metric_count
    raise RuntimeError(
        f'[Log: parse_logs ERROR] Unable to find the "{metric_string}" in any of'
        ' the logs'
    )


def run(
    cluster_project_name,
    cluster_name,
    region,
    job_id,
    arg_project,
    arg_dataset,
    arg_table,
    query,
):
    """Method that calls all the helper function to determine success of a job.

    Args:
      cluster_project_name: Project ID of the GCP project that contains the
        cluster.
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
    cluster_temp_bucket = get_cluster_temp_bucket(
        cluster_project_name, cluster_name, region
    )
    # 2. get the pattern of logs inside the temp bucket in GCS.
    logs_pattern = get_logs_pattern(cluster_project_name, region, job_id)
    # Read the blob and get the metric from them
    metric = read_logs(cluster_temp_bucket, logs_pattern, query)
    bq_table_rows = get_bq_table_row_count(
        cluster_project_name, arg_project, arg_dataset, arg_table, query
    )
    if metric != bq_table_rows:
        raise AssertionError('[Log: parse_logs ERROR] Rows do not match')


def validate_arguments(
    arguments_dictionary, required_arguments, acceptable_arguments
):
    for required_argument in required_arguments:
        if required_argument not in arguments_dictionary:
            raise UserWarning(
                f'[Log: parse_logs ERROR] {required_argument} argument not provided'
            )
    for key, _ in arguments_dictionary.items():
        if key not in acceptable_arguments:
            raise UserWarning(
                f'[Log: parse_logs ERROR] Invalid argument "{key}" provided'
            )


def main(argv: Sequence[str]) -> None:
    acceptable_arguments = {
        'job_id',
        'project_id',
        'cluster_name',
        'region',
        'project_name',
        'dataset_name',
        'table_name',
        'query',
    }
    required_arguments = acceptable_arguments - {'query'}

    if len(argv) > len(acceptable_arguments) + 1:
        raise app.UsageError(
            '[Log: parse_logs ERROR] Too many command-line arguments.'
        )
    elif len(argv) < len(required_arguments) + 1:
        raise app.UsageError(
            '[Log: parse_logs ERROR] Too less command-line arguments.'
        )

    # Arguments are provided of the form "--argument_name=argument_value"
    # We need to extract the name and value as a part of a dictionary.
    # i.e {argument1_name: argument1_value, argument2_name: argument2_value, ...}
    # containing all arguments
    # The pattern
    #     --(\w+)=(.*): Searches for the exact '--'
    #         followed by a group of word character (alphanumeric & underscore)
    #         then an '=' sign
    #         followed by any character except linebreaks

    argument_pattern = r'--(\w+)=(.*)'
    # Forming a dictionary from the arguments
    try:
        matches = [re.match(argument_pattern, argument) for argument in argv[1:]]
        arguments_dictionary = {match.group(1): match.group(2) for match in matches}
        del matches
    except AttributeError as exc:
        raise UserWarning(
            '[Log: parse_logs ERROR] Missing argument value. Please check the '
            'arguments provided again.'
        ) from exc
    # Validating if all necessary arguments are available
    validate_arguments(
        arguments_dictionary, required_arguments, acceptable_arguments
    )
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
        project_id,
        cluster_name,
        region,
        job_id,
        project_name,
        dataset_name,
        table_name,
        query,
    )


if __name__ == '__main__':
    app.run(main)
