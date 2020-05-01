docker build \
  --rm \
  --build-arg AIRFLOW_DEPS="aws" \
  --build-arg AIRFLOW_UI_USER="MTDzi" \
  --build-arg AIRFLOW_UI_PASSWORD="uwolnic_tchorzofretk1" \
  -t puckel/docker-airflow .


# Because I was experimenting with authentication (and adding / removing users),
#  it's important to start with a clean slate
docker-compose -f docker-compose-LocalExecutor.yml rm -s -v


# The -V below stands for:
# -V, --renew-anon-volumes   Recreate anonymous volumes instead of retrieving
#                               data from the previous containers
docker-compose -f docker-compose-LocalExecutor.yml up -V