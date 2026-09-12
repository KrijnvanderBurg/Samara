ARG VARIANT
FROM mcr.microsoft.com/devcontainers/base:${VARIANT} AS base

RUN ZSH_SNIPPET="export HISTFILE=/commandhistory/.zsh_history && export HISTSIZE=10000 && export SAVEHIST=10000" \
    && mkdir /commandhistory \
    && touch /commandhistory/.zsh_history \
    && chown -R ${USER}:${USER} /commandhistory \
    && echo "$ZSH_SNIPPET" >> "/home/${USER}/.zshrc"

# Create Poetry cache directory with correct ownership for vscode user
RUN mkdir -p /home/vscode/.cache/pypoetry \
    && chown -R vscode:vscode /home/vscode/.cache

###  pyspark
# Install Java and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    wget \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Arguments for Spark setup
ARG SPARK_VERSION=4.2.0
ARG HADOOP_VERSION=3

# Set Spark environment variables
ENV SPARK_VERSION=${SPARK_VERSION}
ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Download and install Spark
RUN wget --progress=dot:mega https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
