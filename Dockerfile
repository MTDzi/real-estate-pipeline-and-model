# VERSION 1.10.9
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM python:3.7-slim-buster
LABEL maintainer="Puckel_"

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Airflow
ARG AIRFLOW_VERSION=1.10.9
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ARG AIRFLOW_DEPS=""
ARG PYTHON_DEPS=""

ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Data Engineering Project 5
ARG AIRFLOW_UI_USER
ARG AIRFLOW_UI_PASSWORD
ENV AIRFLOW_UI_USER=$AIRFLOW_UI_USER
ENV AIRFLOW_UI_PASSWORD=$AIRFLOW_UI_PASSWORD

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

# Spark dependencies
ENV APACHE_SPARK_VERSION=2.4.5 \
    HADOOP_VERSION=2.7


# Disable noisy "Handling signal" log messages:
# ENV GUNICORN_CMD_ARGS --log-level WARNING

RUN set -ex \
    && buildDeps=' \
        freetds-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        freetds-bin \
        build-essential \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
        software-properties-common \
        dirmngr \
        apt-transport-https \
        lsb-release \
        ca-certificates \
        gnupg \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${AIRFLOW_USER_HOME} airflow \
    && pip install -U pip setuptools wheel \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install apache-airflow[crypto,celery,postgres,hive,jdbc,mysql,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]==${AIRFLOW_VERSION} \
    && pip install 'redis==3.2' \
    # See: https://github.com/puckel/docker-airflow/issues/535 for the next two lines
    && pip uninstall -y SQLAlchemy \
    && pip install SQLAlchemy==1.3.15 \
    && pip install ipython==7.13.0 \
    && pip install boto3==1.12.41 \
    && pip install sagemaker==1.55.4 \
    && pip install py4j==0.10.7 \
    && pip install pyarrow==0.17.1 \
    # Below, I'm not specifying the version intentionally so that any
    #  critical security updates are incorporated. The version that I worked with
    #  as of typing this was: 0.7.1
    && pip install flask-bcrypt \
    && if [ -n "${PYTHON_DEPS}" ]; then pip install ${PYTHON_DEPS}; fi \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# Install Java
RUN mkdir -p /usr/share/man/man1
RUN echo "deb http://security.debian.org/debian-security stretch/updates main" >> /etc/apt/sources.list
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update -y && \
    apt-get install -y openjdk-8-jdk

RUN apt-get install unzip -y && \
    apt-get autoremove -y

RUN apt update && apt install -y wget

RUN cd /tmp && \
    wget -q $(wget -qO- https://www.apache.org/dyn/closer.lua/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz\?as_json | \
    python -c "import sys, json; content=json.load(sys.stdin); print(content['preferred']+content['path_info'])") && \
    echo "2426a20c548bdfc07df288cd1d18d1da6b3189d0b78dee76fa034c52a4e02895f0ad460720c526f163ba63a17efae4764c46a1cd8f9b04c60f9937a554db85d2 *spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | sha512sum -c - && \
    tar xzf spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local --owner root --group root --no-same-owner && \
    rm spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN cd /usr/local && ln -s spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark

# Spark and Mesos config
ENV SPARK_HOME=/usr/local/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip \
    MESOS_NATIVE_LIBRARY=/usr/local/lib/libmesos.so \
    SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
    PATH=$PATH:$SPARK_HOME/bin


WORKDIR ${AIRFLOW_USER_HOME}


# The following is from: https://pythonspeed.com/articles/activate-virtualenv-dockerfile/
ENV OLD_PATH=$PATH

COPY config/scraper_requirements.txt ${AIRFLOW_USER_HOME}/scraper_requirements.txt
ENV VIRTUAL_ENV=${AIRFLOW_USER_HOME}/scraper_venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$OLD_PATH"
RUN pip install -r scraper_requirements.txt

COPY config/jupyter_requirements.txt ${AIRFLOW_USER_HOME}/jupyter_requirements.txt
ENV VIRTUAL_ENV=${AIRFLOW_USER_HOME}/jupyter_venv
RUN python -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$OLD_PATH"
RUN pip install -r jupyter_requirements.txt

ENV PATH=$OLD_PATH

# Spark-related environmental variables
ENV PYSPARK_DRIVER_PYTHON ipython
ENV PYTHONIOENCODING utf8
ENV PYTHONPATH $SPARK_HOME/python:$SPARK_HOME/python/lib
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH $SPARK_HOME:$SPARK_HOME/bin:$PATH:$JAVA_HOME/bin:$JAVA_HOME/jre


COPY config/airflow.cfg ${AIRFLOW_USER_HOME}/airflow.cfg
COPY config/variables.json ${AIRFLOW_USER_HOME}/variables.json

ADD otodom_scraper ${AIRFLOW_USER_HOME}/otodom_scraper
RUN mkdir ${AIRFLOW_USER_HOME}/scraped_csvs

COPY scripts/entrypoint.sh /entrypoint.sh
COPY scripts/generate_user.py ${AIRFLOW_USER_HOME}/generate_user.py

RUN chown -R airflow: ${AIRFLOW_USER_HOME}

EXPOSE 8080 5555 8793 42000

USER airflow

ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]
