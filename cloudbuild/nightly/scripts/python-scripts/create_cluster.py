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
from google.cloud import dataproc_v1 as dataproc


def create_cluster(project_id, region, cluster_name, num_workers, dataproc_image_version,
                   initialisation_action_script_uri, worker_machine_type):
    """This sample walks a user through creating a Cloud Dataproc cluster
    using the Python client library.

    Args:
        project_id (string): Project to use for creating the cluster.
        region (string): Region where the cluster should live.
        cluster_name (string): Name to use for creating a cluster.
        num_workers (int): Number of workers in the cluster.
        dataproc_image_version (string): The Dataproc Image version used to create the cluster.
        initialisation_action_script_uri (string): Link to the initialisation script for
         the cluster.
        worker_machine_type (string): The Compute Engine machine type used for cluster(worker) instances.
    """
    logging.info(f'Creating cluster {cluster_name} in region {region}')
    # Create a client with the endpoint set to the desired cluster region.
    cluster_client = dataproc.ClusterControllerClient(
        client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'}
    )
    # Create the cluster config.
    cluster = {
        'project_id': project_id,
        'cluster_name': cluster_name,
        'config': {
            'master_config': {'num_instances': 1, 'machine_type_uri': 'n2-standard-4',
                              'disk_config': {'boot_disk_size_gb': 200}},
            'worker_config': {'num_instances': num_workers,
                              'machine_type_uri': worker_machine_type},
            'software_config': {
                'image_version': dataproc_image_version,
                'optional_components': ['FLINK']},
            'initialization_actions': [{'executable_file': initialisation_action_script_uri}],
            'lifecycle_config': {'auto_delete_ttl': '3600s'},
        }
    }
    try:
        # Create the cluster.
        operation = cluster_client.create_cluster(
            request={'project_id': project_id, 'region': region, 'cluster': cluster}
        )
        result = operation.result()
        logging.info(result)
        return True
    except Exception as _:
        logging.info(f'Could not create cluster {cluster} in {region}')
        return False


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
        '--region_array_string',
        dest='region_array_string',
        help='Space separated string of regions.',
        type=str,
        required=True
    )
    parser.add_argument(
        '--dataproc_image_version',
        dest='dataproc_image_version',
        help='The Dataproc Image version used to create the cluster.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--num_workers',
        dest='num_workers',
        help='Number of workers in the cluster.',
        type=int,
        required=True,
    )
    parser.add_argument(
        '--initialisation_action_script_uri',
        dest='initialisation_action_script_uri',
        help='Link to the initialisation script for the cluster.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--region_saving_file',
        dest='region_saving_file',
        help='Location of the file to save the final location in which the cluster is created.',
        type=str,
        required=True
    )
    parser.add_argument(
        '--worker_machine_type',
        dest='worker_machine_type',
        help='The Compute Engine machine type used for cluster(worker) instances.',
        type=str,
        required=True
    )

    args = parser.parse_args(argv[1:])
    # Providing the values.
    cluster_name = args.cluster_name
    region_array = args.region_array_string.split(' ')
    dataproc_image_version = args.dataproc_image_version
    num_workers = int(args.num_workers)
    initialisation_action_script_uri = args.initialisation_action_script_uri
    project_id = args.project_id
    region_saving_file = args.region_saving_file
    worker_machine_type = args.worker_machine_type

    '''This approach retries cluster creation across different regions to address regional CPU 
    quota limitations (i.e., a limit of 24 N2 CPUs per project per region). 
    If a cluster creation attempt fails in one region, the process automatically retries 
    another region from a hardcoded list provided as test variable.
    It throws an error in case cluster was not created in any of the regions'''
    is_cluster_created = False
    for region in region_array:
        logging.info(f'Attempting cluster creation with region {region}')
        is_cluster_created = is_cluster_created or create_cluster(project_id, region, cluster_name,
                                                                  num_workers,
                                                                  dataproc_image_version,
                                                                  initialisation_action_script_uri,
                                                                  worker_machine_type)
        if is_cluster_created:
            file = open(region_saving_file, 'w')
            file.write(region)
            file.close()
            break

    if not is_cluster_created:
        raise RuntimeError(f'The cluster could not be created in any of the '
                           f'{len(region_array)} regions - {region_array}')


if __name__ == '__main__':
    app.run(main)
