# Copyright 2023 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#            http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

import argparse
from absl import logging
from absl import app
from collections.abc import Sequence
import os
from google.cloud import dataproc_v1


def clear_all_dataproc_dependencies(project_id, cluster_name, region):
    delete_active_jobs(project_id, cluster_name, region)
    delete_cluster_buckets(project_id, cluster_name, region)
    delete_cluster(project_id, cluster_name, region)


def delete_active_jobs(project_id, cluster_name, region):
    """Function to list and cancel all running jobs on dataproc cluster.
    Args:
      project_id: the project id to which the cluster and job belongs.
      region: Region to which the cluster belongs.
      cluster_name: Name of the cluster on which the jobs need to be deleted.
    """
    logging.info(f'Attempting to get active job ids on cluster {cluster_name}.')
    # Create the dataproc client.
    client = dataproc_v1.JobControllerClient(
        client_options={
            'api_endpoint': f'{region}-dataproc.googleapis.com:443'
        }
    )
    # Make the list jobs request.
    request = dataproc_v1.ListJobsRequest(
        project_id=project_id,
        cluster_name=cluster_name,
        region=region,
        filter='status.state = ACTIVE',
    )
    active_jobs = client.list_jobs(request=request)
    active_job_ids = [active_job.reference.job_id for active_job in active_jobs]

    for job_id in active_job_ids:
        # Cancel all the active jobs.
        cancel_dataproc_request = dataproc_v1.CancelJobRequest(
            project_id=project_id,
            region=region,
            job_id=job_id,
        )
        client.cancel_job(request=cancel_dataproc_request)
        logging.info(
            'Cancelled dataproc job with id'
            f' "{job_id}", in project "{project_id}" and region'
            f' "{region}"'
        )
    logging.info(f'Cancelled all active jobs on cluster "{cluster_name}".')


def delete_cluster_buckets(project_id, cluster_name, region):
    """Function to list and cancel all running jobs on dataproc cluster.
    Args:
      project_id: the project id to which the cluster and job belongs.
      region: Region to which the cluster belongs.
      cluster_name: Name of the cluster on which the jobs need to be deleted.
    """
    logging.info(f'Attempting to delete cluster {cluster_name} buckets')
    cluster_client = dataproc_v1.ClusterControllerClient(
        client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'}
    )
    get_cluster_request = dataproc_v1.GetClusterRequest(
        project_id=project_id, region=region, cluster_name=cluster_name
    )
    cluster_resource = cluster_client.get_cluster(get_cluster_request)

    cluster_id = cluster_resource.cluster_uuid
    temp_bucket_location = cluster_resource.config.temp_bucket+"/"+cluster_id
    staging_bucket_location = cluster_resource.config.config_bucket+("/google-cloud-dataproc"
                                                                     "-metainfo/")+cluster_id
    delete_bucket(temp_bucket_location)
    delete_bucket(staging_bucket_location)
    logging.info(f'Delete all the buckets connected to cluster {cluster_name}.')


def delete_bucket(bucket_name):
    logging.info(f'Attempting to delete the bucket {bucket_name}')
    os.system(f'gcloud storage rm --recursive gs://{bucket_name}')
    logging.info(f'Bucket {bucket_name} deleted')


def delete_cluster(project_id, cluster_name, region):
    """Function to list and cancel all running jobs on dataproc cluster.
    Args:
      project_id: the project id to which the cluster and job belongs.
      region: Region to which the cluster belongs.
      cluster_name: Name of the cluster on which the jobs need to be deleted.
    """
    logging.info(f'Attempting to delete the cluster {cluster_name}')
    # Create the cluster client.
    cluster_client = dataproc_v1.ClusterControllerClient(
        client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'}
    )
    # Delete the cluster once the job has terminated.
    operation = cluster_client.delete_cluster(
        request={
            'project_id': project_id,
            'region': region,
            'cluster_name': cluster_name,
        }
    )
    operation.result()
    logging.info(f'Cluster {cluster_name} successfully deleted.')


def main(argv: Sequence[str]) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--cluster_name',
        dest='cluster_name',
        help='Name of the cluster to be created.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--project_id',
        dest='project_id',
        help='Project Id in which the cluster is created.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--region',
        dest='region',
        help='Region in which the cluster is present.',
        type=str,
        required=True,
    )

    args = parser.parse_args(argv[1:])
    # Providing the values.
    cluster_name = args.cluster_name
    region = args.region
    project_id = args.project_id

    clear_all_dataproc_dependencies(project_id, cluster_name, region)


if __name__ == '__main__':
    app.run(main)
