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
from absl import app
from collections.abc import Sequence

from google.cloud import dataproc_v1 as dataproc


def create_cluster(project_id, region, cluster_name, num_workers, dataproc_image_version,
                   initialisation_action_script_uri, staging_bucket_name, temp_bucket_name):
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
        staging_bucket_name (string): Location of the staging bucket for the cluster.
        temp_bucket_name (string): Location of the temporary bucket for the cluster
    """
    # Create a client with the endpoint set to the desired cluster region.
    cluster_client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )

    # TODO: --enable-component-gateway.
    # Create the cluster config.
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "config_bucket": staging_bucket_name,
            "temp_bucket": temp_bucket_name,
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": num_workers, "machine_type_uri": "n2-standard-4"},
            "software_config": {
                "image_version": dataproc_image_version,
                "optional_components": ["FLINK"]},
            "initialization_actions": [{"executable_file": initialisation_action_script_uri}],
            "lifecycle_config": {"auto_delete_ttl": '3600s'},
        }
    }
    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": project_id, "region": region, "cluster": cluster}
    )

    try:
        result = operation.result()
    except Exception as e:
        raise RuntimeError("Cluster could not be created.")


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
        help='Region to create the cluster.',
        type=str,
        required=True,
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
        '--temp_bucket_name',
        dest='temp_bucket_name',
        help='Location of the temporary bucket for the cluster.',
        type=str,
        required=True,
    )
    parser.add_argument(
        '--staging_bucket_name',
        dest='staging_bucket_name',
        help='Location of the staging bucket for the cluster.',
        type=str,
        required=True,
    )
    args = parser.parse_args(argv[1:])

    # Providing the values.
    cluster_name = args.cluster_name
    region = args.region
    dataproc_image_version = args.dataproc_image_version
    num_workers = int(args.num_workers)
    initialisation_action_script_uri = args.initialisation_action_script_uri
    project_id = args.project_id
    staging_bucket_name = args.staging_bucket_name
    temp_bucket_name=args.temp_bucket_name

    create_cluster(project_id, region, cluster_name, num_workers, dataproc_image_version,
                   initialisation_action_script_uri, temp_bucket_name, staging_bucket_name)


if __name__ == '__main__':
    app.run(main)
