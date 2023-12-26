"""Module to cancel any running dataproc jobs in a cluster."""
from collections.abc import Sequence

from absl import app
from google.cloud import dataproc_v1

from utils import utils


def cancel_dataproc_job(project_id, cluster_region_name, job_id):
    """Function to cancel a given dataproc job.

    Args:
      project_id: the project id to which the cluster and job belongs.
      cluster_region_name: the cluster to which
      job_id: job_id of the job that needs to be cancelled.
    """
    # Create a client
    client = dataproc_v1.JobControllerClient(
        client_options={
            'api_endpoint': f'{cluster_region_name}-dataproc.googleapis.com:443'
        }
    )

    # Initialize request argument(s)
    request = dataproc_v1.CancelJobRequest(
        project_id=project_id,
        region=cluster_region_name,
        job_id=job_id,
    )

    # Make the request
    client.cancel_job(request=request)

    # Handle the response
    print(
        '[Log: cancel_dataproc_job INFO] Cancelled dataproc job with id'
        f' "{job_id}", in project "{project_id}" and region'
        f' "{cluster_region_name}"'
    )


def get_active_job_ids(project_id, cluster_name, cluster_region_name):
    # Create a client
    client = dataproc_v1.JobControllerClient(
        client_options={
            'api_endpoint': f'{cluster_region_name}-dataproc.googleapis.com:443'
        }
    )

    # Initialize request argument(s)
    request = dataproc_v1.ListJobsRequest(
        project_id=project_id,
        cluster_name=cluster_name,
        region=cluster_region_name,
        filter='status.state = ACTIVE',
    )

    # Make the request
    active_jobs = client.list_jobs(request=request)
    active_job_ids = [active_job.reference.job_id for active_job in active_jobs]

    return active_job_ids


def main(argv: Sequence[str]) -> None:
    required_arguments = {'project_id', 'cluster_name', 'cluster_region_name'}

    arg_input_utils = utils.ArgumentInputUtils(
        argv, required_arguments, required_arguments
    )
    arguments_dictionary = arg_input_utils.input_validate_and_return_arguments()

    project_id = arguments_dictionary['project_id']
    cluster_name = arguments_dictionary['cluster_name']
    cluster_region_name = arguments_dictionary['cluster_region_name']

    # Getting all the active jobs.
    active_job_ids = get_active_job_ids(
        project_id, cluster_name, cluster_region_name
    )
    # Cancelling them.
    for active_job_id in active_job_ids:
        cancel_dataproc_job(project_id, cluster_region_name, active_job_id)


if __name__ == '__main__':
    app.run(main)
