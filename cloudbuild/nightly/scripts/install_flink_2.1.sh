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

# The Dataproc agent constructs the classpath for Flink jobs by globbing /usr/lib/flink/lib/*.
# It natively ignores the HADOOP_CLASSPATH environment variable inside flink-env.sh.
# Since Flink 2.0 no longer bundles YARN/Hadoop support, we must symlink the Hadoop and YARN client jars directly into Flink s lib directory so Dataproc natively loads them.
find /usr/lib/hadoop/ /usr/lib/hadoop-hdfs/ /usr/lib/hadoop-mapreduce/ /usr/lib/hadoop-yarn/ -type f -name "*.jar" ! -name "*test*" ! -name "commons-cli-*" ! -name "slf4j-*" ! -name "log4j-*" ! -name "reload4j-*" -exec ln -sf {} /usr/lib/flink/lib/ \; 2>/dev/null || true
# Flink 2.0+ deprecated and moved YARN support out of the main distribution.
# Dataproc requires YARN to submit jobs, so we must manually download the flink-yarn plugin.
wget -q -O /usr/lib/flink/lib/flink-yarn-${FLINK_VERSION}.jar "https://repo1.maven.org/maven2/org/apache/flink/flink-yarn/${FLINK_VERSION}/flink-yarn-${FLINK_VERSION}.jar"

ln -sf /usr/lib/flink/bin/flink /usr/bin/flink

# Restart Flink HistoryServer and YARN session
# Force the YARN session and Flink clients to use a shared, predictable properties file 
# rather than a user-specific one (e.g. /tmp/.yarn-properties-root vs /tmp/.yarn-properties-flink)
echo "yarn.properties-file.location: /tmp/.yarn-properties-dataproc" >> /usr/lib/flink/conf/flink-conf.yaml
echo "yarn.properties-file.location: /tmp/.yarn-properties-dataproc" >> /usr/lib/flink/conf/config.yaml || true

systemctl restart flink-history-server || true
systemctl restart flink-yarn-session || true

echo "Flink ${FLINK_VERSION} installation complete."
