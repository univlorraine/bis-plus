#!/usr/bin/env python3
"""
Script de migration automatique de dags/utils/ vers plugins/amue/
Réorganise les fichiers par responsabilité et met à jour les imports
"""

import os
import shutil
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple


class AMUEMigrationTool:
    """Outil de migration vers la structure plugins"""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.base_dir = Path.cwd()
        self.utils_dir = self.base_dir / 'dags' / 'utils'
        self.plugins_dir = self.base_dir / 'plugins' / 'amue'

        # Mapping des fichiers
        self.file_mapping = {
            # Hooks
            'amue_api_hook.py': 'hooks/amue_api_hook.py',

            # Services
            'amue_status_checker.py': 'services/status_checker.py',
            'amue_polling_service.py': 'services/polling_service.py',
            'amue_metadata_manager.py': 'services/metadata_manager.py',

            # Operators
            'amue_table_filter.py': 'operators/table_filter.py',
            'amue_table_verifier.py': 'operators/table_verifier.py',
            'amue_table_manager.py': 'operators/table_manager.py',
            'amue_data_importer.py': 'operators/data_importer.py',

            # Notifications
            'amue_notification_utils.py': 'notifications/notification_service.py',
            'amue_report_generator.py': 'notifications/report_generator.py',

            # Utils
            'amue_utils.py': 'utils/transformers.py',
        }

        # Règles de remplacement des imports
        self.import_replacements = {
            # Ancien -> Nouveau
            'from utils import': 'from amue import',
            'from .amue_api_hook import': 'from amue.hooks import',
            'from .amue_status_checker import': 'from amue.services import',
            'from .amue_polling_service import': 'from amue.services import',
            'from .amue_metadata_manager import': 'from amue.services import',
            'from .amue_table_filter import': 'from amue.operators import',
            'from .amue_table_verifier import': 'from amue.operators import',
            'from .amue_table_manager import': 'from amue.operators import',
            'from .amue_data_importer import': 'from amue.operators import',
            'from .amue_notification_utils import': 'from amue.notifications import',
            'from .amue_notifications import': 'from amue.notifications import',
            'from .amue_report_generator import': 'from amue.notifications import',
            'from .amue_utils import': 'from amue.utils import',
            'from .amue_transformers import': 'from amue.utils import',
        }

    def log(self, message: str, level: str = 'INFO'):
        """Affiche un message de log"""
        prefix = '🔍' if self.dry_run else '✓'
        if level == 'WARN':
            prefix = '⚠'
        elif level == 'ERROR':
            prefix = '❌'
        print(f"{prefix} [{level}] {message}")

    def create_backup(self) -> Path:
        """Crée une sauvegarde de dags/utils/"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = self.base_dir / 'backups' / f'utils_backup_{timestamp}'

        if self.dry_run:
            self.log(f"[DRY RUN] Créerait sauvegarde: {backup_dir}")
            return backup_dir

        backup_dir.mkdir(parents=True, exist_ok=True)

        if self.utils_dir.exists():
            for file in self.utils_dir.glob('*.py'):
                shutil.copy2(file, backup_dir / file.name)
                self.log(f"Sauvegarde: {file.name}")

        self.log(f"Sauvegarde créée: {backup_dir}")
        return backup_dir

    def create_directory_structure(self):
        """Crée la structure de répertoires plugins/amue/"""
        directories = [
            self.plugins_dir,
            self.plugins_dir / 'hooks',
            self.plugins_dir / 'services',
            self.plugins_dir / 'operators',
            self.plugins_dir / 'notifications',
            self.plugins_dir / 'utils',
        ]

        for directory in directories:
            if self.dry_run:
                self.log(f"[DRY RUN] Créerait: {directory}")
            else:
                directory.mkdir(parents=True, exist_ok=True)
                self.log(f"Répertoire créé: {directory}")

    def migrate_file(self, source_name: str, target_path: str) -> bool:
        """Migre un fichier et met à jour ses imports"""
        source_file = self.utils_dir / source_name
        target_file = self.plugins_dir / target_path

        if not source_file.exists():
            self.log(f"Fichier source non trouvé: {source_name}", 'WARN')
            return False

        # Lecture du contenu
        with open(source_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # Mise à jour des imports
        updated_content = self.update_imports(content)

        if self.dry_run:
            self.log(f"[DRY RUN] Migrerait: {source_name} -> {target_path}")
            return True

        # Écriture du nouveau fichier
        target_file.parent.mkdir(parents=True, exist_ok=True)
        with open(target_file, 'w', encoding='utf-8') as f:
            f.write(updated_content)

        self.log(f"Migré: {source_name} -> {target_path}")
        return True

    def update_imports(self, content: str) -> str:
        """Met à jour les imports dans un fichier"""
        updated = content

        for old_import, new_import in self.import_replacements.items():
            updated = updated.replace(old_import, new_import)

        # Remplace les imports relatifs restants
        updated = re.sub(
            r'from \. import ([A-Za-z_, ]+)',
            r'from amue import \1',
            updated
        )

        return updated

    def create_init_files(self):
        """Crée tous les fichiers __init__.py"""

        init_files = {
            # Principal
            '__init__.py': self._get_main_init_content(),

            # Hooks
            'hooks/__init__.py': '''"""
Hooks AMUE - Connexions et authentification
"""

from .amue_api_hook import AMUEAPIHook

__all__ = ['AMUEAPIHook']
''',

            # Services
            'services/__init__.py': '''"""
Services AMUE - Logique métier
"""

from .status_checker import AMUEStatusChecker
from .polling_service import AMUEPollingService, PollingConfig, PollingResult
from .metadata_manager import AMUEMetadataManager, TableMetadata

__all__ = [
    'AMUEStatusChecker',
    'AMUEPollingService',
    'PollingConfig',
    'PollingResult',
    'AMUEMetadataManager',
    'TableMetadata',
]
''',

            # Operators
            'operators/__init__.py': '''"""
Operators AMUE - Opérations de traitement
"""

from .table_filter import AMUETableFilter, TableNotFoundError
from .table_verifier import AMUETableVerifier
from .table_manager import AMUETableManager, TableManagementResult
from .data_importer import AMUEDataImporter

__all__ = [
    'AMUETableFilter',
    'TableNotFoundError',
    'AMUETableVerifier',
    'AMUETableManager',
    'TableManagementResult',
    'AMUEDataImporter',
]
''',

            # Notifications
            'notifications/__init__.py': '''"""
Notifications AMUE - Communication et rapports
"""

from .notification_service import (
    NotificationService,
    ErrorContext,
    send_failure_notification
)
from .report_generator import AMUEReportGenerator

__all__ = [
    'NotificationService',
    'ErrorContext',
    'send_failure_notification',
    'AMUEReportGenerator',
]
''',

            # Utils
            'utils/__init__.py': '''"""
Utils AMUE - Fonctions utilitaires
"""

from .transformers import (
    parse_column_definition,
    compute_structure_hash,
    compute_structure_hash_with_pk,
    format_primary_keys,
    compare_fingerprints
)

__all__ = [
    'parse_column_definition',
    'compute_structure_hash',
    'compute_structure_hash_with_pk',
    'format_primary_keys',
    'compare_fingerprints',
]
''',
        }

        for file_path, content in init_files.items():
            full_path = self.plugins_dir / file_path

            if self.dry_run:
                self.log(f"[DRY RUN] Créerait: {file_path}")
            else:
                full_path.parent.mkdir(parents=True, exist_ok=True)
                with open(full_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                self.log(f"Créé: {file_path}")

    def _get_main_init_content(self) -> str:
        """Retourne le contenu du __init__.py principal"""
        return '''"""
Plugin AMUE pour Airflow
Import automatique de données depuis l'API AMUE vers PostgreSQL
"""

from airflow.plugins_manager import AirflowPlugin

# Import des hooks
from amue.hooks import AMUEAPIHook

# Import des services
from amue.services import (
    AMUEStatusChecker,
    AMUEPollingService,
    AMUEMetadataManager
)

# Import des operators
from amue.operators import (
    AMUETableFilter,
    AMUETableVerifier,
    AMUETableManager,
    AMUEDataImporter
)

# Import des notifications
from amue.notifications import (
    NotificationService,
    ErrorContext,
    send_failure_notification,
    AMUEReportGenerator
)

# Import des utils
from amue.utils import (
    parse_column_definition,
    compute_structure_hash,
    compute_structure_hash_with_pk,
    format_primary_keys,
    compare_fingerprints
)


class AMUEPlugin(AirflowPlugin):
    """Plugin AMUE pour Airflow"""
    name = "amue"
    hooks = [AMUEAPIHook]


__all__ = [
    'AMUEAPIHook',
    'AMUEStatusChecker',
    'AMUEPollingService',
    'AMUEMetadataManager',
    'AMUETableFilter',
    'AMUETableVerifier',
    'AMUETableManager',
    'AMUEDataImporter',
    'NotificationService',
    'ErrorContext',
    'send_failure_notification',
    'AMUEReportGenerator',
    'parse_column_definition',
    'compute_structure_hash',
    'compute_structure_hash_with_pk',
    'format_primary_keys',
    'compare_fingerprints',
]
'''

    def update_dag_file(self):
        """Met à jour le fichier DAG principal"""
        dag_file = self.base_dir / 'dags' / 'dag_amue_dynamic_table.py'

        if not dag_file.exists():
            self.log("Fichier DAG non trouvé", 'WARN')
            return

        with open(dag_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # Remplace l'import de utils par amue
        old_import = 'from utils import ('
        new_import = 'from amue import ('

        if old_import in content:
            content = content.replace(old_import, new_import)

            if self.dry_run:
                self.log(f"[DRY RUN] Mettrait à jour: {dag_file.name}")
            else:
                with open(dag_file, 'w', encoding='utf-8') as f:
                    f.write(content)
                self.log(f"DAG mis à jour: {dag_file.name}")
        else:
            self.log("Import 'from utils' non trouvé dans le DAG", 'WARN')

    def run_migration(self) -> bool:
        """Exécute la migration complète"""
        print("=" * 70)
        print("Migration de dags/utils/ vers plugins/amue/")
        if self.dry_run:
            print("MODE DRY RUN - Aucune modification ne sera effectuée")
        print("=" * 70)
        print()

        try:
            # 1. Sauvegarde
            print("Étape 1/5: Sauvegarde")
            print("-" * 70)
            backup_dir = self.create_backup()
            print()

            # 2. Création de la structure
            print("Étape 2/5: Création de la structure")
            print("-" * 70)
            self.create_directory_structure()
            print()

            # 3. Migration des fichiers
            print("Étape 3/5: Migration des fichiers")
            print("-" * 70)
            migrated = 0
            for source, target in self.file_mapping.items():
                if self.migrate_file(source, target):
                    migrated += 1
            self.log(f"Total: {migrated}/{len(self.file_mapping)} fichiers migrés")
            print()

            # 4. Création des __init__.py
            print("Étape 4/5: Création des fichiers __init__.py")
            print("-" * 70)
            self.create_init_files()
            print()

            # 5. Mise à jour du DAG
            print("Étape 5/5: Mise à jour du DAG principal")
            print("-" * 70)
            self.update_dag_file()
            print()

            # Résumé
            print("=" * 70)
            if self.dry_run:
                print("✓ Dry run terminé avec succès")
                print()
                print("Pour exécuter réellement la migration:")
                print("  python3 migrate_to_plugins.py --execute")
            else:
                print("✓ Migration terminée avec succès")
                print()
                print("Prochaines étapes:")
                print("1. Redémarrer Airflow: ./manage.sh restart")
                print("2. Vérifier les plugins: docker-compose exec airflow-apiserver airflow plugins")
                print("3. Vérifier les DAGs: ./manage.sh dags")
                print("4. Tester l'import: python3 -c 'from amue import AMUEAPIHook; print(\"✓ OK\")'")
                print(f"\nEn cas de problème, restaurez depuis: {backup_dir}")
            print("=" * 70)

            return True

        except Exception as e:
            self.log(f"Erreur lors de la migration: {str(e)}", 'ERROR')
            import traceback
            traceback.print_exc()
            return False


def main():
    """Point d'entrée principal"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Migre dags/utils/ vers plugins/amue/'
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help='Exécute réellement la migration (sans cette option, mode dry-run)'
    )

    args = parser.parse_args()

    # Par défaut en dry-run
    dry_run = not args.execute

    migration = AMUEMigrationTool(dry_run=dry_run)
    success = migration.run_migration()

    return 0 if success else 1


if __name__ == '__main__':
    import sys

    sys.exit(main())