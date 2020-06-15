**NOTE:** this repo is actually a copy (*not* a fork) of the [`puckel/docker-airflow`](https://github.com/puckel/docker-airflow)
project, however since I had one [other](https://github.com/MTDzi/data_nanodegree_project_5) repo that's also its fork,
and I didn't want this project to be merely a branch of that other repo, I made a hard copy and
now you're looking at it.

# Introduction

This project is comprised of three main components which correspond to three Airflow DAGs:
1. A daily scraping job defined in `dags/otodom_scraping_dag.py` that scrapes the **`otodom.pl`** website for real estate
   deals in Warsaw (Poland). The result of this scraping is a CSV file that is then de-duplicated, cleaned up, checked for quality,
   joined with a
   dataset containing population density (see point 2.), and finally the resulting data frame has its columns renamed (translated to English),
   to then be dumped as a .parquet file. Each CSV is roughly ~30MB large, and contains about 30.000 rows of data.
   This is the DAG as seen in Airflow's Graph View ![](images/otodom_dag.png)
2. An on-demand pipeline for scraping the **`mapa.um.warszawa.pl`** website. This is a simple DAG, comprising two tasks:
   the first one scrapes the website using the Python `requests` package, and the second that does a quality check and 
   cleans up the data to then finally dump it into a .parquet file.
   This is the DAG as seen in Airflow's Graph View ![](images/warsaw_map_dag.png)
3. A weekly pipeline defined in `dags/spark_dedup_dag.py` that takes the .parquet files produced thus far, and combines
   them into a single .parquet file with deduplicated rows (using the `link` column).
   This is the DAG as seen in Airflow's Graph View ![](images/spark_dedup_dag.png)
   
## Purpose
The purpose of this project was to create a pipeline that scrapes data from two sources, combines them, cleanes them up,
and stores to be then used to train a machine learning model that, for example, assesses the price of a real estate.
The data are also deduplicated using Spark (which was needed since the whole data no longer fits in memory), and eventually
stored as a single frame, again, in a .parquet file.

The data are scraped daily, and as of writing this I have data from about 90 days (70 of those scraped manually, and the
remaining 20 scraped automatically by Airflow), which amounts to about 90 x 30.000 = 2.700.000 rows.
After deduplication this number shrinks to about 40.000,  

   
## Data model
The data model is a single table stored as a .parquet file in a data lake that has the following layout (this is a transposed view generated
with the following command: `print(final_frame.head(3).T.to_markdown())`):

|                              | 0                                                                                  | 1                                                                                    | 2                                                               |
|:-----------------------------|:-----------------------------------------------------------------------------------|:-------------------------------------------------------------------------------------|:----------------------------------------------------------------|
| address                      | Warszawa, Wilanów, Zawady, Bruzdowa                                                | Warszawa, Ochota, al. Aleje Jerozolimskie                                            | Warszawa, Śródmieście, Żelazna                                  |
| balcony                      | 1                                                                                  | 0                                                                                    | 0                                                               |
| price                        | 860540.0                                                                           | 482553                                                                               | 560000.0                                                        |
| rent                         |                                                                                    | nan                                                                                  |                                                                 |
| scraped_at                   | 2020-01-03 13:10:48.931666                                                         | 2020-05-01 19:34:08.916578                                                           | 2020-05-19 08:45:52.136266                                      |
| antiburglar_door_windows     | 0                                                                                  | 0                                                                                    | 0                                                               |
| two_floors                   | 0                                                                                  | 0                                                                                    | 0                                                               |
| form_of_property             | pełna własność                                                                     | nan                                                                                  | pełna własność                                                  |
| garage_parking_spot          | 1                                                                                  | 1                                                                                    | 0                                                               |
| air_conditioning             | 0                                                                                  | 0                                                                                    | 0                                                               |
| lat                          | 52.17229575                                                                        | 52.2032596                                                                           | 52.234668799999994                                              |
| number_of_floors_in_building | 2.0                                                                                | nan                                                                                  | 12.0                                                            |
| number_of_rooms              | 5                                                                                  | 2                                                                                    | 2                                                               |
| link                         | https://www.otodom.pl/oferta/101mkw-5-pokoi-duzy-taras-wilanow-zawady-ID441Ca.html | https://www.otodom.pl/oferta/2-luksus-pokoje-inwestycja-z-duzym-rabatem-ID45DLz.html | https://www.otodom.pl/oferta/2-pok-37-5-m2-centrum-ID45TAx.html |
| lon                          | 21.1126586                                                                         | 20.938595                                                                            | 20.9907818                                                      |
| building_material            | nan                                                                                | nan                                                                                  | nan                                                             |
| surveillance_security        | 0                                                                                  | 0                                                                                    | 0                                                               |
| separate_kitchen             | 0                                                                                  | 0                                                                                    | 0                                                               |
| garden                       | 0                                                                                  | 0                                                                                    | 0                                                               |
| type_of_heating              | nan                                                                                | nan                                                                                  | miejskie                                                        |
| type_of_windows              | plastikowe                                                                         | nan                                                                                  | plastikowe                                                      |
| floor                        | 2                                                                                  | 1                                                                                    | nan                                                             |
| basement                     | 0                                                                                  | 0                                                                                    | 0                                                               |
| storage_room                 | 0                                                                                  | 0                                                                                    | 0                                                               |
| area_in_m2                   | 101.24                                                                             | 51.61                                                                                | 37.5                                                            |
| type_of_building             | apartamentowiec                                                                    | apartamentowiec                                                                      | blok                                                            |
| construction_year            | 2021.0                                                                             | nan                                                                                  |                                                                 |
| market                       | pierwotny                                                                          | pierwotny                                                                            | wtórny                                                          |
| finish                       | do wykończenia                                                                     | do wykończenia                                                                       | nan                                                             |
| alarm_system                 | 0                                                                                  | 0                                                                                    | 0                                                               |
| terrace                      | 0                                                                                  | 0                                                                                    | 0                                                               |
| closed_area                  | 0                                                                                  | 0                                                                                    | 0                                                               |
| elevator                     | 0                                                                                  | 0                                                                                    | 0                                                               |
| population_density           | 0.6705883045991261                                                                 | 0.45254907011985773                                                                  | 0.11458975076675415                                             |

### What if the data was increased 100x ?
There are two stages which might be impacted by this:
1. The scraping stage
2. The Spark deduplication

For the first stage, the CSV file produced would change its size from ~30MB to ~3GB which is still within the range
that would easily fit into memory. However, if this was a limiting factor, I would need to modify the scraping
module that's being used as backend (https://github.com/pixinixi/otodom_scraper), and since I've already contributed
to this project this shouldn't be a problem.

For the second stage, the duplication is being done on Spark DataFrames, where one of those frames comprises
data coming from a one day scrape of the otodom.pl website, and the second is the combined data thus far.
The firs frame is, again, about ~30MB in size, and the second is constantly growing, and currently is at about 45MB.
On the one hand, Spark can easily deal with this amount of data, and even if we had to process correspondingly
~3GB and 4.5GB of data, this would not be an issue. One the other hand, the current approach is not optimal
and can be made more efficient by using the [Delta Lake](https://docs.databricks.com/delta/delta-intro.html) library.

### What if the pipeline were to run every day, at 7 am?
This is not a problem since I used Airflow for scheduling.

### What if the database needed to be accessed by 100+ people?
Since the data are stored as a data lake, this is not an issue in the sense that the data will be available.
The users wouldn't be able to directly modify the data, but they would be able to analyze them.
However, as a data lake it would require an ELT pipeline, which would require computational resources in the form of an
EMR cluster or, more broadly, a Spark cluster for loading these data.

## Future work
Currently, the pipeline runs on a local machine, within a docker container spun up with `docker-compose`.
The scraped CSV files and processed .parquet files are stored locally, instead of an S3 bucket because I wanted to avoid additional costs.
Also, the Spark job is being done locally, within the container.

Future work includes:

0. Using Data Lakes for the Spark upsert job
1. Pushing the resulting .parquet files to an actual S3 bucket
2. Spinning up an EMR cluster for running the spark job for deduplication
3. Deploying Airflow to ECS and have the whole scheduling done on cloud
   
As mentioned, I wanted to avoid additional costs, but the solution was implemented with the last three steps in mind.
The most difficult part would be deploying Airflow to ECS, I think, because both points 1. and 2. were already done in the course.

For now, my plan is to share this solution with others in a blog post, and then expand on it by moving it step-by-step
to the cloud.

# How to build the `docker-airflow` image

## Before you build it

In this repo's main directory run:

    git submodule init
    git submodule update
    
to get the `otodom_scraper` repo which contains scripts for scraping the real estate data. BTW, I'm running the scraper
by using the `BashOperator`, which in turn needs to run within a virtualenv (that contains `scrapy`, for example) -- for
details refer to the Dockerfile and the `dags/otodom_scraping_dag.py` script.

The `docker build` command can be found in the `all_in_one.sh` script, but this is its rough layout: 

    docker build \
        --rm \
        --build-arg AIRFLOW_DEPS="aws" \
        --build-arg AIRFLOW_UI_USER="some_user_name" \
        --build-arg AIRFLOW_UI_PASSWORD="some_password" \
        -t puckel/docker-airflow .
    
The "some_user_name" and its corresponding password "some_password" will be needed for logging in into Airflow.

## Get it to run

There's a script, `all_in_one.sh`, that does all the work, check it out to see the steps.

## More about the DAGs



---

**What follows is the original README from the `puckle/docker-airflow` repo.**

---

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
