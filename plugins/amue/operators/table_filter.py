"""
Filtrage et sélection des tables AMUE à importer
Avec arrêt et notification si table absente du statut
"""
import json
import logging
from datetime import datetime
from typing import Dict, List

from airflow.exceptions import AirflowException
from amue.notifications.notification_service import NotificationService, ErrorContext, send_failure_notification
from amue.utils.airflow_helpers import AirflowVariableManager as VarMgr

logger = logging.getLogger(__name__)


class TableNotFoundError(AirflowException):
    """Exception levée quand une table configurée n'est pas trouvée dans le statut"""

    def __init__(self, missing_tables: List[str], configured_count: int, found_count: int):
        self.missing_tables = missing_tables
        self.configured_count = configured_count
        self.found_count = found_count

        message = (
            f"ERREUR CRITIQUE: {len(missing_tables)} table(s) configurée(s) "
            f"absente(s) du statut API.\n"
            f"Tables configurées: {configured_count}\n"
            f"Tables trouvées: {found_count}\n"
            f"Tables manquantes: {', '.join(missing_tables)}"
        )

        super().__init__(message)


class AMUETableFilter:
    """Filtre les tables à traiter selon leur statut et historique"""

    def __init__(self, tables_config: List[Dict] = None):
        if tables_config is None:
            tables_config = self._load_config()
        self.tables_config = tables_config

        # Initialise le service de notification si disponible
        if NotificationService is not None:
            self.notification_service = NotificationService()
        else:
            self.notification_service = None

    def filter_tables(self, current_status: Dict) -> List[Dict]:
        """
        Filtre les tables selon statut actuel

        Vérifie que toutes les tables configurées existent dans le statut.
        Si des tables sont manquantes, envoie une notification et arrête le traitement.

        Args:
            current_status: Statut actuel de l'API (dict avec noms de tables en clés)

        Returns:
            Liste des tables à traiter

        Raises:
            TableNotFoundError: Si des tables configurées sont absentes du statut
        """
        logger.info(f"[FILTER] Filtrage de {len(self.tables_config)} tables configurées")

        # Vérification CRITIQUE: toutes les tables configurées doivent exister dans le statut
        missing_tables = self._check_tables_exist_in_status(current_status)

        if missing_tables:
            # Envoi de la notification d'erreur
            self._send_missing_tables_notification(missing_tables, current_status)

            # Lève une exception pour arrêter le DAG
            raise TableNotFoundError(
                missing_tables=missing_tables,
                configured_count=len(self.tables_config),
                found_count=len(current_status)
            )

        # Si toutes les tables existent, on continue le filtrage normal
        tables_to_process = []

        for table_config in self.tables_config:
            if not isinstance(table_config, dict) or 'name' not in table_config:
                continue

            table_name = table_config['name'].upper()

            # Enrichit la config
            enriched_config = self._enrich_table_config(
                table_config,
                current_status[table_name]
            )

            # Détermine si on traite cette table
            if self._should_process_table(enriched_config):
                tables_to_process.append(enriched_config)
                logger.info(f"[FILTER] {table_name}: À traiter ({enriched_config['import_type']})")
            else:
                logger.info(f"[FILTER] {table_name}: Skip")

        logger.info(f"[FILTER] {len(tables_to_process)} tables à traiter")
        return tables_to_process

    def _check_tables_exist_in_status(self, current_status: Dict) -> List[str]:
        """
        Vérifie que toutes les tables configurées existent dans le statut API

        Args:
            current_status: Statut actuel de l'API

        Returns:
            Liste des tables manquantes (vide si tout OK)
        """
        missing_tables = []

        for table_config in self.tables_config:
            if not isinstance(table_config, dict) or 'name' not in table_config:
                continue

            table_name = table_config['name'].upper()
            if (table_name not in current_status) or (current_status[table_name]['status'] != 'OK'):
                logger.warning(f"Table '{table_name}' absente du statut API ou status != OK")
                missing_tables.append(table_name)
            else:
                logger.info(f"Table '{table_name}' trouvée dans le statut")

        return missing_tables

    def _send_missing_tables_notification(self, missing_tables: List[str], current_status: Dict) -> None:
        """
        Envoie une notification d'erreur pour les tables manquantes

        Args:
            missing_tables: Liste des tables absentes
            current_status: Statut actuel pour information
        """
        logger.info("Envoi notification pour tables manquantes")

        # Construction du message d'erreur détaillé
        error_message = self._build_missing_tables_error_message(missing_tables, current_status)

        # Si NotificationService n'est pas disponible, utiliser le fallback
        if self.notification_service is None or ErrorContext is None:
            logger.warning("NotificationService non disponible, utilisation du fallback")
            logger.error(error_message)

            # Tenter d'utiliser send_failure_notification si disponible
            try:
                # Créer un contexte minimal
                context = {
                    'task_instance': type('obj', (object,), {
                        'dag_id': 'amue_multi_table_import',
                        'task_id': 'filter_tables_to_process'
                    }),
                    'exception': TableNotFoundError(
                        missing_tables=missing_tables,
                        configured_count=len(self.tables_config),
                        found_count=len(current_status)
                    )
                }
                send_failure_notification(context)
            except Exception as e:
                logger.warning(f"[{type(e).__name__}] Impossible d'envoyer la notification: {str(e)}")
            return

        # Contexte d'erreur
        error_context = ErrorContext(
            execution_date=datetime.now().isoformat(),
            dag_id='amue_multi_table_import',
            task_id='filter_tables_to_process',
            error_message=error_message,
            error_type='TableNotFoundError',
            status='failed'
        )

        # Envoi de la notification
        try:
            self.notification_service.send_error_notification(error_context)
            logger.info("[FILTER] Notification envoyée avec succès")
        except Exception as e:
            logger.warning(f"[{type(e).__name__}] Échec envoi notification: {str(e)}")

    def _build_missing_tables_error_message(self, missing_tables: List[str], current_status: Dict) -> str:
        """
        Construit un message d'erreur détaillé pour les tables manquantes

        Args:
            missing_tables: Tables absentes
            current_status: Statut actuel

        Returns:
            Message d'erreur formaté
        """
        message_lines = [
            "=" * 70,
            "ERREUR CRITIQUE: Tables configurées absentes du statut API",
            "=" * 70,
            "",
            f"Nombre de tables configurées : {len(self.tables_config)}",
            f"Nombre de tables dans le statut API : {len(current_status)}",
            f"Nombre de tables manquantes : {len(missing_tables)}",
            "",
            "Tables manquantes :",
        ]

        for table in missing_tables:
            message_lines.append(f"  - {table}")

        message_lines.extend([
            "",
            "Tables disponibles dans l'API :",
        ])

        for table in sorted(list(current_status.keys())[:20]):  # Limite à 20 pour lisibilité
            status_info = current_status[table]
            message_lines.append(f"  - {table} (status: {status_info['status']})")

        if len(current_status) > 20:
            message_lines.append(f"  ... et {len(current_status) - 20} autres tables")

        message_lines.extend([
            "",
            "Actions à effectuer :",
            "1. Vérifier que les noms de tables dans la configuration sont corrects",
            "2. Vérifier que les tables existent côté AMUE",
            "3. Contacter l'administrateur AMUE si les tables devraient être disponibles",
            "=" * 70
        ])

        return "\n".join(message_lines)

    def _load_config(self) -> List[Dict]:
        """Charge la configuration des tables depuis les variables"""
        default_config = json.dumps([{
            "name": "CSKS",
            "primary_key": "",
            "delta": "",
            "last_import": "",
            "finger_print": ""
        }])

        tables_var = VarMgr.get('amue_tables_to_import', default=default_config)
        tables_config = json.loads(tables_var) if isinstance(tables_var, str) else tables_var

        return tables_config if isinstance(tables_config, list) else []

    def _enrich_table_config(self, table_config: Dict, current_status: Dict) -> Dict:
        """
        Enrichit la config d'une table avec le statut actuel

        Récupère automatiquement les clés primaires si absentes
        """
        enriched = table_config.copy()

        # Ajoute les valeurs par défaut
        enriched.setdefault('primary_key', '')
        enriched.setdefault('delta', '')
        enriched.setdefault('last_import', '')
        enriched.setdefault('finger_print', '')

        # Récupération automatique des clés primaires si absentes
        if not enriched.get('primary_key') or not enriched['primary_key'].strip():
            logger.info(f"[FILTER] Table {enriched['name']}: Clés primaires absentes, récupération via API")
            enriched['needs_pk_update'] = True
        else:
            enriched['needs_pk_update'] = False

        # Ajoute le statut actuel
        enriched['current_status'] = current_status

        return enriched

    def _should_process_table(self, table_config: Dict) -> bool:
        """Détermine si une table doit être traitée"""
        current_status = table_config['current_status']['status']

        if current_status != 'OK':
            return False

        # Détermine le type d'import
        has_last_import = bool(table_config.get('last_import'))
        has_delta = bool(table_config.get('delta'))

        table_config['import_type'] = 'differential' if (has_last_import and has_delta) else 'full'
        table_config['to_process'] = True

        return True
