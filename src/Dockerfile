ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.10

FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

RUN apt-get update
RUN apt-get install -y --no-install-recommends git wget

WORKDIR /opt/spark/work-dir/

COPY --from=py3 / /

RUN wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.0.0/iceberg-spark-runtime-3.3_2.12-1.0.0.jar -P /opt/spark/

# Cache Dependencies Steps
COPY ./requirements-docker.txt .
RUN pip install -r requirements-docker.txt

# Only rebuild the source code package definition to run the tests
COPY . .
RUN pip install -e .


CMD ["pytest"]