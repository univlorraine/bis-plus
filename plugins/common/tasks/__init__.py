"""Tasks partagées entre les DAGs AMUE et ECC."""
from common.tasks.restore_inactive import restore_inactive

__all__ = ['restore_inactive']
