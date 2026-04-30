# BaseTemplates vit désormais dans common.notifications.base_templates pour
# qu'ECC puisse en hériter sans dépendre d'amue. Re-exporté ici pour
# préserver les imports existants `from amue.notifications.templates_base
# import BaseTemplates`.
from common.notifications.base_templates import BaseTemplates  # noqa: F401
