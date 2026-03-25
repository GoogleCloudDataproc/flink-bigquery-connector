#!/bin/bash

# Copyright 2024 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

# This script replaces the default Dataproc Flink installation with Flink 2.1.0
# Dataproc optional components install BEFORE initialization actions.
# This means /usr/lib/flink already exists when this script runs.

set -euxo pipefail

export FLINK_VERSION="2.1.0"
export FLINK_TAR="flink-${FLINK_VERSION}-bin-scala_2.12.tgz"
export DOWNLOAD_URL="https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/${FLINK_TAR}"

wget -q -O "/tmp/${FLINK_TAR}" "${DOWNLOAD_URL}"
tar -xzf "/tmp/${FLINK_TAR}" -C /usr/lib

# Stop any running Flink HistoryServer or processes
systemctl stop flink-history-server || true

mv /usr/lib/flink /usr/lib/flink-dataproc
mv "/usr/lib/flink-${FLINK_VERSION}" /usr/lib/flink

# Copy Dataproc configuration so YARN and History Server integration works
cp -a /usr/lib/flink-dataproc/conf/* /usr/lib/flink/conf/

# The Dataproc environment relies on Hadoop, but standard Flink 2.1 does not bundle it.
# We must ensure Flink explicitly sets HADOOP_CLASSPATH from the master node.
echo "export HADOOP_CLASSPATH=\$(hadoop classpath)" >> /usr/lib/flink/conf/flink-env.sh

# Flink 2.0+ deprecated and moved YARN support out of the main distribution.
# Dataproc requires YARN to submit jobs, so we must manually download the flink-yarn plugin.
wget -q -O /usr/lib/flink/lib/flink-yarn-${FLINK_VERSION}.jar "https://repo1.maven.org/maven2/org/apache/flink/flink-yarn/${FLINK_VERSION}/flink-yarn-${FLINK_VERSION}.jar"

ln -sf /usr/lib/flink/bin/flink /usr/bin/flink

# Restart Flink HistoryServer and YARN session
systemctl restart flink-history-server || true
systemctl restart flink-yarn-session || true

echo "Flink ${FLINK_VERSION} installation complete."
