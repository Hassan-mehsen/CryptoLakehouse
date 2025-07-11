# airflow image
FROM apache/airflow:3.0.2-python3.10

# Set environment variables for Spark and Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_VERSION=3.5.6
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:$PATH"

# Install Java & Spark with root rights
USER root
USER root
RUN echo "deb http://deb.debian.org/debian bullseye main" > /etc/apt/sources.list.d/bullseye.list && \
    apt-get update && \
    apt-get install -y openjdk-11-jdk curl build-essential && \
    java -version && \
    apt-get clean

RUN curl -fsSL https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# Back to normal user
USER airflow

WORKDIR /opt/airflow

# Copy the code in the image
COPY ./dags/ dags/
COPY ./src/ src/
COPY ./alembic/ alembic/
COPY ./requirements.txt .
 COPY .env .env 

# Install all python dependencies
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
