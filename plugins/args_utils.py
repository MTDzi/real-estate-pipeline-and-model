from typing import Dict, Any
from datetime import datetime, timedelta
from airflow.models import Variable


def get_default_args() -> Dict[str, Any]:
    default_args = Variable.get('default_args', deserialize_json=True)
    default_args['start_date'] = datetime(*default_args['start_date_list'])
    default_args['retry_delay'] = timedelta(minutes=default_args['retry_delay_in_minutes'])
    return default_args
