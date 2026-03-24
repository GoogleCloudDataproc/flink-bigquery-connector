#!/bin/bash

# Copyright 2024 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License");

# This script replaces the default Dataproc Flink installation with Flink 2.1.0
# Dataproc initialization actions run BEFORE optional components.
# This means /usr/lib/flink does not exist when this script runs.
# We spawn a background daemon to wait for Dataproc Flink to finish installing,
# then we perform the swap.

set -euxo pipefail

cat << 'INNER_EOF' > /usr/local/bin/swap_flink.sh
#!/bin/bash
set -x

# Wait for the optional component to install Flink
while [ ! -f /usr/lib/flink/bin/flink ]; do
  sleep 5
done

# Wait for Dataproc to finish whatever it's doing so we don't disrupt it
sleep 15

export FLINK_VERSION="2.1.0"
export FLINK_TAR="flink-${FLINK_VERSION}-bin-scala_2.12.tgz"
export DOWNLOAD_URL="https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/${FLINK_TAR}"

wget -q -O "/tmp/${FLINK_TAR}" "${DOWNLOAD_URL}"
tar -xzf "/tmp/${FLINK_TAR}" -C /usr/lib

mv /usr/lib/flink /usr/lib/flink-dataproc
mv "/usr/lib/flink-${FLINK_VERSION}" /usr/lib/flink

# Copy Dataproc configuration so YARN and History Server integration works
cp -a /usr/lib/flink-dataproc/conf/* /usr/lib/flink/conf/
ln -sf /usr/lib/flink/bin/flink /usr/bin/flink

echo "Flink ${FLINK_VERSION} installation complete."
INNER_EOF

chmod +x /usr/local/bin/swap_flink.sh
nohup /usr/local/bin/swap_flink.sh > /var/log/swap_flink.log 2>&1 &
