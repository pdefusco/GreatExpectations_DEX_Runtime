FROM docker-private.infra.cloudera.com/cloudera/dex/dex-spark-runtime-2.4.7-7.1.7.2000:1.19.1-b70

USER root

RUN yum install -y git && yum clean all && rm -rf /var/cache/yum
RUN yum -y update && yum install wget -y
RUN wget https://www.python.org/ftp/python/3.7.2/Python-3.7.2.tgz && tar xzf Python-3.7.2.tgz
RUN cd Python-3.7.2 && ./configure --enable-optimizations

#RUN rm -f /usr/bin/python && ln -s /usr/bin/python /usr/bin/python3

RUN pip2 install virtualenv-api
RUN pip3 install virtualenv-api
RUN pip3 install --upgrade pip
RUN pip3 install great_expectations
USER pauldefusco
