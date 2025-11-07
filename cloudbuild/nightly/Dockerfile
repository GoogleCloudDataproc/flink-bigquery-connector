# This Dockerfile creates an image for running presubmit tests.
# This Dockerfile creates an image for running presubmit tests.
FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    ca-certificates \
    openjdk-11-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN \
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | \
  tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
  curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
  apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
  apt-get update -y && \
  apt-get install google-cloud-cli -y \
  && apt clean
# Install Python and Basic Python Tools
RUN apt-get -y install python3 && apt clean
# Get pip to download and install requirements
RUN apt-get -y install python3-pip && apt clean
# For md5sum
RUN apt-get -y install coreutils && apt clean
COPY ./cloudbuild/nightly/scripts/python-scripts/requirements.txt /workspace/
RUN pip3 install -r /workspace/requirements.txt
