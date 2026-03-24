#!/bin/bash

# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script replaces the default Dataproc Flink installation with Flink 2.1.0
# to allow integration testing of Flink 2.1 connectors.

set -euxo pipefail

export FLINK_VERSION="2.1.0"
export FLINK_TAR="flink-${FLINK_VERSION}-bin-scala_2.12.tgz"
export DOWNLOAD_URL="https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/${FLINK_TAR}"

echo "Downloading Flink ${FLINK_VERSION} from ${DOWNLOAD_URL}..."
wget -q -O "/tmp/${FLINK_TAR}" "${DOWNLOAD_URL}"

echo "Extracting Flink ${FLINK_VERSION}..."
tar -xzf "/tmp/${FLINK_TAR}" -C /usr/lib

echo "Replacing Dataproc Flink with Flink ${FLINK_VERSION}..."
# Backup original Dataproc Flink
mv /usr/lib/flink /usr/lib/flink-dataproc

# Move Flink 2.1.0 to /usr/lib/flink
mv "/usr/lib/flink-${FLINK_VERSION}" /usr/lib/flink

# Copy Dataproc configuration so YARN and History Server integration works
cp -a /usr/lib/flink-dataproc/conf/* /usr/lib/flink/conf/

# The bin symlinks on Dataproc already point to /usr/lib/flink/bin/flink
ln -sf /usr/lib/flink/bin/flink /usr/bin/flink

echo "Flink ${FLINK_VERSION} installation complete."
