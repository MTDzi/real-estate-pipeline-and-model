**NOTE:** this repo is actually a copy of the [`puckel/docker-airflow`](https://github.com/puckel/docker-airflow)
project, however since I had one [other](https://github.com/MTDzi/data_nanodegree_project_5) repo that's also its fork,
and I didn't want this project to be merely a branch of that other repo, I made a hard copy and
now you're looking at it.

# Introduction

This project is comprised of three main components which correspond to three Airflow DAGs:
1. A daily scraping job defined in `dags/otodom_scraping_dag.py` that scrapes the **`otodom.pl`** website for real estate
   deals in Warsaw (Poland). The result of this scraping is a CSV file that is then de-duplicated, cleaned up, joined with
   dataset containing population density, and finally the resulting data frame has its columns renamed (translated to English),
   to then be dumped as a .parquet file. This is the DAG as seen in Airflow's Graph View ![](images/otodom_dag.png)
2. An on-demand pipeline for scraping the **`mapa.um.warszawa.pl`** website. This is a simple DAG, comprised of two tasks:
   the first one scrapes the website using the Python `requests` package, and the second task cleans up the data and dumps
   them into a .parquet file. This is the DAG as seen in Airflow's Graph View ![](images/warsaw_map_dag.png)
3. A weekly pipeline defined in `dags/emr_cleanup_dag.py` that spins up an EMR cluster on which several Spark
   jobs are ran that clean up the data, find and remove duplicates, and load the data back to the S3 data lake.
3. An on-demand pipeline for training a model defined in `dags/training_dag.py` whose primary purpose is *not* to train a
   model, but rather to create an analysis based on the embeddings that this model provides. 

More details about each of this components are given below.

## How to build the `docker-airflow` image

### Before you build it

In this repo's main directory run:

    git submodule init
    git submodule update
    
to get the `otodom_scraper` repo which contains scripts for scraping the real estate data. BTW, I'm running the scraper
by using the `BashOperator`, which in turn needs to run within a virtualenv (that contains scrapy, for example) which is
being set up in the Dockerfile.

Since I'm using the [`airflow.providers.amazon.aws`](https://airflow.readthedocs.io/en/latest/_api/airflow/providers/amazon/aws/operators/index.html),
subpackage, I'm building the image with an additional `AIRFLOW_DEPS`, like so: 

    docker build \
        --rm \
        --build-arg AIRFLOW_DEPS="aws" \
        --build-arg AIRFLOW_UI_USER="some_user_name" \
        --build-arg AIRFLOW_UI_PASSWORD="some_password" \
        -t puckel/docker-airflow .
    

## IaC for setting up the Redshift service

I used the `exploration.ipynb` notebook (which I took from my solution [here](https://github.com/MTDzi/data_nanodegree_project_3))
for setting up Redshift, the tables, as well as deleting the Redshift cluster
once everything is done (although in the end it was easier to just kill the cluster in the AWS console).

## Get it to run

There's a script, `all_in_one.sh`, that does all the work, please check it out to see the steps.
However, that's not all, you'll still need to set up the connections in the Airflow UI.
Initially, my plan was to have those connections defined in a file of sorts but thus far I've failed to find
a secure way of doing that, so I left that as a future improvement.

Before running the main DAG, in the Airflow UI you need to:
 1. Define a `"aws_default"` connection
 2. Define a `"redshift"` connection
 
And after that, turn the DAG "On", and click "Toggle DAG", and... that's pretty

**What follows is the original README from the `puckle/docker-airflow` repo.**

# docker-airflow
[![CI status](https://github.com/puckel/docker-airflow/workflows/CI/badge.svg?branch=master)](https://github.com/puckel/docker-airflow/actions?query=workflow%3ACI+branch%3Amaster+event%3Apush)
[![Docker Build status](https://img.shields.io/docker/build/puckel/docker-airflow?style=plastic)](https://hub.docker.com/r/puckel/docker-airflow/tags?ordering=last_updated)

[![Docker Hub](https://img.shields.io/badge/docker-ready-blue.svg)](https://hub.docker.com/r/puckel/docker-airflow/)
[![Docker Pulls](https://img.shields.io/docker/pulls/puckel/docker-airflow.svg)]()
[![Docker Stars](https://img.shields.io/docker/stars/puckel/docker-airflow.svg)]()

This repository contains **Dockerfile** of [apache-airflow](https://github.com/apache/incubator-airflow) for [Docker](https://www.docker.com/)'s [automated build](https://registry.hub.docker.com/u/puckel/docker-airflow/) published to the public [Docker Hub Registry](https://registry.hub.docker.com/).

## Informations

* Based on Python (3.7-slim-buster) official Image [python:3.7-slim-buster](https://hub.docker.com/_/python/) and uses the official [Postgres](https://hub.docker.com/_/postgres/) as backend and [Redis](https://hub.docker.com/_/redis/) as queue
* Install [Docker](https://www.docker.com/)
* Install [Docker Compose](https://docs.docker.com/compose/install/)
* Following the Airflow release from [Python Package Index](https://pypi.python.org/pypi/apache-airflow)

## Installation

Pull the image from the Docker repository.

    docker pull puckel/docker-airflow

## Build

Optionally install [Extra Airflow Packages](https://airflow.incubator.apache.org/installation.html#extra-package) and/or python dependencies at build time :

    docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" -t puckel/docker-airflow .
    docker build --rm --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .

or combined

    docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .

Don't forget to update the airflow images in the docker-compose files to puckel/docker-airflow:latest.

## Usage

By default, docker-airflow runs Airflow with **SequentialExecutor** :

    docker run -d -p 8080:8080 puckel/docker-airflow webserver

If you want to run another executor, use the other docker-compose.yml files provided in this repository.

For **LocalExecutor** :

    docker-compose -f docker-compose-LocalExecutor.yml up -d

For **CeleryExecutor** :

    docker-compose -f docker-compose-CeleryExecutor.yml up -d

NB : If you want to have DAGs example loaded (default=False), you've to set the following environment variable :

`LOAD_EX=n`

    docker run -d -p 8080:8080 -e LOAD_EX=y puckel/docker-airflow

If you want to use Ad hoc query, make sure you've configured connections:
Go to Admin -> Connections and Edit "postgres_default" set this values (equivalent to values in airflow.cfg/docker-compose*.yml) :
- Host : postgres
- Schema : airflow
- Login : airflow
- Password : airflow

For encrypted connection passwords (in Local or Celery Executor), you must have the same fernet_key. By default docker-airflow generates the fernet_key at startup, you have to set an environment variable in the docker-compose (ie: docker-compose-LocalExecutor.yml) file to set the same key accross containers. To generate a fernet_key :

    docker run puckel/docker-airflow python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)"

## Configuring Airflow

It's possible to set any configuration value for Airflow from environment variables, which are used over values from the airflow.cfg.

The general rule is the environment variable should be named `AIRFLOW__<section>__<key>`, for example `AIRFLOW__CORE__SQL_ALCHEMY_CONN` sets the `sql_alchemy_conn` config option in the `[core]` section.

Check out the [Airflow documentation](http://airflow.readthedocs.io/en/latest/howto/set-config.html#setting-configuration-options) for more details

You can also define connections via environment variables by prefixing them with `AIRFLOW_CONN_` - for example `AIRFLOW_CONN_POSTGRES_MASTER=postgres://user:password@localhost:5432/master` for a connection called "postgres_master". The value is parsed as a URI. This will work for hooks etc, but won't show up in the "Ad-hoc Query" section unless an (empty) connection is also created in the DB

## Custom Airflow plugins

Airflow allows for custom user-created plugins which are typically found in `${AIRFLOW_HOME}/plugins` folder. Documentation on plugins can be found [here](https://airflow.apache.org/plugins.html)

In order to incorporate plugins into your docker container
- Create the plugins folders `plugins/` with your custom plugins.
- Mount the folder as a volume by doing either of the following:
    - Include the folder as a volume in command-line `-v $(pwd)/plugins/:/usr/local/airflow/plugins`
    - Use docker-compose-LocalExecutor.yml or docker-compose-CeleryExecutor.yml which contains support for adding the plugins folder as a volume

## Install custom python package

- Create a file "requirements.txt" with the desired python modules
- Mount this file as a volume `-v $(pwd)/requirements.txt:/requirements.txt` (or add it as a volume in docker-compose file)
- The entrypoint.sh script execute the pip install command (with --user option)

## UI Links

- Airflow: [localhost:8080](http://localhost:8080/)
- Flower: [localhost:5555](http://localhost:5555/)


## Scale the number of workers

Easy scaling using docker-compose:

    docker-compose -f docker-compose-CeleryExecutor.yml scale worker=5

This can be used to scale to a multi node setup using docker swarm.

## Running other airflow commands

If you want to run other airflow sub-commands, such as `list_dags` or `clear` you can do so like this:

    docker run --rm -ti puckel/docker-airflow airflow list_dags

or with your docker-compose set up like this:

    docker-compose -f docker-compose-CeleryExecutor.yml run --rm webserver airflow list_dags

You can also use this to run a bash shell or any other command in the same environment that airflow would be run in:

    docker run --rm -ti puckel/docker-airflow bash
    docker run --rm -ti puckel/docker-airflow ipython

# Simplified SQL database configuration using PostgreSQL

If the executor type is set to anything else than *SequentialExecutor* you'll need an SQL database.
Here is a list of PostgreSQL configuration variables and their default values. They're used to compute
the `AIRFLOW__CORE__SQL_ALCHEMY_CONN` and `AIRFLOW__CELERY__RESULT_BACKEND` variables when needed for you
if you don't provide them explicitly:

| Variable            | Default value |  Role                |
|---------------------|---------------|----------------------|
| `POSTGRES_HOST`     | `postgres`    | Database server host |
| `POSTGRES_PORT`     | `5432`        | Database server port |
| `POSTGRES_USER`     | `airflow`     | Database user        |
| `POSTGRES_PASSWORD` | `airflow`     | Database password    |
| `POSTGRES_DB`       | `airflow`     | Database name        |
| `POSTGRES_EXTRAS`   | empty         | Extras parameters    |

You can also use those variables to adapt your compose file to match an existing PostgreSQL instance managed elsewhere.

Please refer to the Airflow documentation to understand the use of extras parameters, for example in order to configure
a connection that uses TLS encryption.

Here's an important thing to consider:

> When specifying the connection as URI (in AIRFLOW_CONN_* variable) you should specify it following the standard syntax of DB connections,
> where extras are passed as parameters of the URI (note that all components of the URI should be URL-encoded).

Therefore you must provide extras parameters URL-encoded, starting with a leading `?`. For example:

    POSTGRES_EXTRAS="?sslmode=verify-full&sslrootcert=%2Fetc%2Fssl%2Fcerts%2Fca-certificates.crt"

# Simplified Celery broker configuration using Redis

If the executor type is set to *CeleryExecutor* you'll need a Celery broker. Here is a list of Redis configuration variables
and their default values. They're used to compute the `AIRFLOW__CELERY__BROKER_URL` variable for you if you don't provide
it explicitly:

| Variable          | Default value | Role                           |
|-------------------|---------------|--------------------------------|
| `REDIS_PROTO`     | `redis://`    | Protocol                       |
| `REDIS_HOST`      | `redis`       | Redis server host              |
| `REDIS_PORT`      | `6379`        | Redis server port              |
| `REDIS_PASSWORD`  | empty         | If Redis is password protected |
| `REDIS_DBNUM`     | `1`           | Database number                |

You can also use those variables to adapt your compose file to match an existing Redis instance managed elsewhere.

# Wanna help?

Fork, improve and PR.
