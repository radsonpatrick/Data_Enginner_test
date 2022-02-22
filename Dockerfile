FROM apache/airflow:2.2.3
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         default-jre \
         libreoffice
USER airflow