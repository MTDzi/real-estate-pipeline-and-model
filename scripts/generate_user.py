import os
from airflow import models, settings
from airflow.contrib.auth.backends.password_auth import PasswordUser


user = PasswordUser(models.User())
user.username = os.environ['AIRFLOW_UI_USER']
user.password = os.environ['AIRFLOW_UI_PASSWORD']
user.superuser = True

# session = settings.Session()
#
# q = session.query(models.User).filter(models.User.username != user.username)
# print(session.query(q.exists()).all())
#
# try:
#     session.add(user)
#     session.commit()
# except Exception:
#     pass
# finally:
#     session.close()
