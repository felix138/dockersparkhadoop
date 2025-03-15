FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

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
# 创建 Hadoop 日志目录
RUN mkdir -p /var/log/hadoop && chmod 777 /var/log/hadoop

# 设置环境变量
ENV HADOOP_LOG_DIR=/var/log/hadoop
ENV HDFS_CONF_dfs_replication=3
HEALTHCHECK CMD curl -f http://localhost:9870/ || exit 1

ENV HDFS_CONF_dfs_namenode_name_dir=file:///hadoop/dfs/name
RUN mkdir -p /hadoop/dfs/name
VOLUME /hadoop/dfs/name
RUN chmod -R 777 /hadoop/dfs/name
ADD namenode/run.sh /run.sh
RUN chmod a+x /run.sh

EXPOSE 9870
EXPOSE 9000

CMD ["/run.sh"]
