import os
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser


user = PasswordUser(models.User())
user.username = os.environ['AIRFLOW_UI_USER']
user.password = os.environ['AIRFLOW_UI_PASSWORD']
user.superuser = True

session = settings.Session()
session.add(user)
session.commit()
session.close()
