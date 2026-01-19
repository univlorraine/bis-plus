from copy import deepcopy
from pydantic.utils import deep_update
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from pydantic.v1.utils import deep_update

LOGGING_CONFIG = deep_update(deepcopy(DEFAULT_LOGGING_CONFIG),
        {
            "loggers": {
                "airflow.models.dagbag": {
                    "level": "WARNING",
                    "propagate": False,
                },
                "airflow.dag_processing": {
                    "level": "WARNING",
                    "propagate": False,
                },
                "airflow.dag_processing.bundles.manager": {
                    "level": "WARNING",
                    "propagate": False,
                },
            }
        }
    )