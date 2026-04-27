"""Defaults partagés pour tous les DAGs du projet."""
from datetime import timedelta

import pendulum


DEFAULT_START_DATE = pendulum.datetime(2024, 1, 1, tz="Europe/Paris")


def standard_default_args(on_failure_callback=None, **overrides):
    """Renvoie un default_args standard pour @dag.

    Args:
        on_failure_callback: callback task-level (injecté dans default_args).
        **overrides: clés surchargées (retries, retry_delay, owner, ...).
    """
    args = {
        'owner': 'airflow',
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    }
    if on_failure_callback is not None:
        args['on_failure_callback'] = on_failure_callback
    args.update(overrides)
    return args
