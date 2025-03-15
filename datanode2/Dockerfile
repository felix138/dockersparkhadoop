FROM bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8

USER root

RUN echo 'deb http://archive.debian.org/debian/ stretch main contrib non-free' > /etc/apt/sources.list && \
    echo 'deb-src http://archive.debian.org/debian/ stretch main contrib non-free' >> /etc/apt/sources.list && \
    echo 'deb http://archive.debian.org/debian-security stretch/updates main' >> /etc/apt/sources.list && \
    echo 'deb-src http://archive.debian.org/debian-security stretch/updates main' >> /etc/apt/sources.list && \
    apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      openjdk-8-jdk \
      net-tools \
      curl \
      netcat \
      gnupg \
      libsnappy-dev \
      tzdata \
      iputils-ping \
      telnet \
      procps \
      vim \ 
    && rm -rf /var/lib/apt/lists/*
      
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/

HEALTHCHECK CMD curl -f http://localhost:9864/ || exit 1

ENV HDFS_CONF_dfs_datanode_data_dir=file:///hadoop/dfs/data
RUN mkdir -p /hadoop/dfs/data
VOLUME /hadoop/dfs/data


ADD datanode/run.sh /run.sh
RUN chmod a+x /run.sh
# 在Dockerfile末尾添加
RUN chmod -R 777 /hadoop/dfs/data


EXPOSE 9864 9866 9867
CMD ["/run.sh"]



