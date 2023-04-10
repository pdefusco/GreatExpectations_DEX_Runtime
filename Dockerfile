FROM docker-private.infra.cloudera.com/cloudera/dex/dex-spark-runtime-2.4.8-7.2.15.0:1.18.1-h3-b6
RUN groupadd -r pauldefusco && useradd -r -g pauldefusco pauldefusco
#FROM docker-private.infra.cloudera.com/cloudera/dex/dex-spark-runtime-2.4.7-7.2.7.2000:1.19.1-b70
#FROM container.repository.cloudera.com/cloudera/dex/dex-spark-runtime-2.4.8-7.2.15.0:1.18.1-h3-b6

USER root
RUN yum install ${YUM_OPTIONS} gcc openssl-devel libffi-devel bzip2-devel wget python39 python39-devel && yum clean all && rm -rf /var/cache/yum
RUN update-alternatives --remove-all python
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1
RUN rm /usr/bin/python3
RUN ln -s /usr/bin/python3.9 /usr/bin/python3
RUN yum install python39-pip
RUN /usr/bin/python3.9 -m pip install dbt-core==1.3.1 dbt-impala==1.3.1 dbt-hive==1.3.1 impyla==0.18.0 confluent-kafka[avro,json,protobuf]==1.9.2 great_expectations

USER pauldefusco
