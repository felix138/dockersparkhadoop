FROM bde2020/spark-worker:3.0.0-hadoop3.2
#apt update and install kits

RUN apk update && apk add --no-cache \
    openjdk8 \
    net-tools \
    curl \
    netcat-openbsd \
    gnupg \
    snappy-dev \
    tzdata \
    iputils \
    busybox-extras \
    procps \
    vim
# Set timezone to Oslo
RUN ln -sf /usr/share/zoneinfo/Europe/Oslo /etc/localtime && \
    echo "Europe/Oslo" > /etc/timezone
RUN wget -P /spark/jars/ https://repo1.maven.org/maven2/com/zaxxer/HikariCP/5.0.1/HikariCP-5.0.1.jar
RUN wget -P /spark/jars/ https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar
