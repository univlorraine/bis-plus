"""
Layer: application

Detection et logging des doublons de cle primaire.

Module générique (AMUE / ECC) : détecte les doublons de clé primaire dans
un batch avant insertion, permettant de diagnostiquer les problèmes de
qualité de données.
"""
import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class DuplicateDetector:
    """
    Detecte et log les doublons de cle primaire dans les batches de donnees.

    Permet de:
    - Detecter proactivement les doublons AVANT insertion en base
    - Analyser les conflits de cle primaire apres erreur UniqueViolation
    - Generer des logs detailles pour faciliter le diagnostic

    Example:
        >>> detector = DuplicateDetector()
        >>> duplicates = detector.detect_duplicates_in_batch(batch, columns, primary_keys)
        >>> if duplicates:
        ...     detector.log_batch_duplicates('CSKS', columns, primary_keys, duplicates)
    """

    def detect_duplicates_in_batch(
        self,
        batch: List[tuple],
        columns: List[str],
        primary_keys: List[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Detecte TOUS les doublons de cle primaire dans le batch.

        Args:
            batch: Liste des tuples de donnees
            columns: Liste des noms de colonnes
            primary_keys: Liste des colonnes formant la cle primaire

        Returns:
            Dict avec cle PK (stringifiee) -> liste des lignes en doublon.
            Seuls les groupes avec plus d'une ligne sont retournes.
        """
        try:
            pk_indices = self._get_pk_indices(columns, primary_keys)
            if not pk_indices:
                return {}

            # Groupe les lignes par cle primaire
            pk_groups: Dict[str, List[Dict[str, Any]]] = {}

            for idx, record in enumerate(batch):
                pk_key, pk_values = self._build_pk_key(record, pk_indices)

                if pk_key not in pk_groups:
                    pk_groups[pk_key] = []

                row_dict = dict(zip(columns, record))
                row_dict['_batch_index'] = idx
                row_dict['_pk_values'] = pk_values
                pk_groups[pk_key].append(row_dict)

            # Retourne uniquement les groupes avec doublons
            return {k: v for k, v in pk_groups.items() if len(v) > 1}

        except Exception as e:
            logger.warning(f"[CONFLIT PK] Erreur detection doublons: {e}")
            return {}

    def find_duplicates_for_pk(
        self,
        batch: List[tuple],
        columns: List[str],
        primary_keys: List[str],
        pk_values: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """
        Trouve TOUTES les lignes avec une cle primaire specifique dans le batch.

        Args:
            batch: Liste des tuples de donnees
            columns: Liste des noms de colonnes
            primary_keys: Liste des colonnes formant la cle primaire
            pk_values: Valeurs de la cle primaire a rechercher

        Returns:
            Liste des lignes correspondantes (avec _batch_index)
        """
        duplicates = []

        try:
            pk_indices = self._get_pk_indices(columns, primary_keys)

            for idx, record in enumerate(batch):
                if self._matches_pk(record, pk_indices, pk_values):
                    row_dict = dict(zip(columns, record))
                    row_dict['_batch_index'] = idx
                    duplicates.append(row_dict)

            return duplicates

        except Exception as e:
            logger.warning(f"[CONFLIT PK] Erreur recherche doublons dans batch: {e}")
            return []

    def extract_pk_from_error(
        self,
        error_message: str,
        primary_keys: List[str]
    ) -> Optional[Dict[str, str]]:
        """
        Extrait les valeurs de cle primaire depuis un message d'erreur PostgreSQL.

        Le message ressemble a:
        DETAIL: La cle « (col1, col2)=(val1, val2) » existe deja.

        Args:
            error_message: Message d'erreur PostgreSQL
            primary_keys: Liste des colonnes de la cle primaire

        Returns:
            Dict des valeurs de PK ou None si extraction echouee
        """
        try:
            # Pattern pour extraire les colonnes et valeurs
            pattern = r'\(([^)]+)\)=\(([^)]+)\)'
            match = re.search(pattern, error_message)

            if not match:
                logger.warning("[CONFLIT PK] Impossible d'extraire les valeurs PK du message d'erreur")
                return None

            cols_str = match.group(1)
            vals_str = match.group(2)

            cols = [c.strip() for c in cols_str.split(',')]
            vals = self._parse_pk_values(vals_str)

            if len(cols) != len(vals):
                logger.warning(f"[CONFLIT PK] Mismatch colonnes/valeurs: {len(cols)} vs {len(vals)}")
                return None

            pk_values = dict(zip(cols, vals))
            logger.info(f"[CONFLIT PK] Cle primaire en conflit: {pk_values}")

            return pk_values

        except Exception as e:
            logger.warning(f"[CONFLIT PK] Erreur extraction PK: {e}")
            return None

    def log_batch_duplicates(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        duplicates_groups: Dict[str, List[Dict[str, Any]]]
    ) -> None:
        """
        Affiche tous les groupes de doublons trouves dans le batch.

        Args:
            table_name: Nom de la table
            columns: Liste des colonnes
            primary_keys: Liste des cles primaires
            duplicates_groups: Dict des groupes de doublons
        """
        separator = "=" * 100
        total_duplicates = sum(len(group) for group in duplicates_groups.values())

        logger.error(separator)
        logger.error(f"DOUBLONS DETECTES DANS LES DONNEES API - Table: {table_name}")
        logger.error(f"ATTENTION: {len(duplicates_groups)} groupe(s) de doublons, {total_duplicates} lignes au total!")
        logger.error(separator)

        for group_idx, (pk_key, duplicates) in enumerate(duplicates_groups.items(), 1):
            self._log_duplicate_group(group_idx, duplicates, columns, primary_keys)

        logger.error(separator)
        logger.error("RESUME:")
        logger.error(f"  - {len(duplicates_groups)} groupe(s) de doublons")
        logger.error(f"  - {total_duplicates} lignes en doublon au total")
        logger.error("ACTION RECOMMANDEE:")
        logger.error("  1. Verifier les donnees source dans l'API AMUE")
        logger.error("  2. Contacter l'administrateur AMUE pour signaler les doublons")
        logger.error("  3. OU utiliser l'option UPSERT (import differentiel) pour gerer les doublons")
        logger.error(separator)

    def log_api_duplicates(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        duplicates: List[Dict[str, Any]],
        pk_values: Dict[str, str]
    ) -> None:
        """
        Affiche les doublons trouves dans les donnees de l'API.

        Args:
            table_name: Nom de la table
            columns: Liste des colonnes
            primary_keys: Liste des cles primaires
            duplicates: Liste des lignes en doublon
            pk_values: Valeurs de la cle primaire dupliquee
        """
        separator = "=" * 100
        logger.error(separator)
        logger.error(f"DOUBLONS DANS LES DONNEES API - Table: {table_name}")
        logger.error(f"ATTENTION: L'API a renvoye {len(duplicates)} lignes avec la meme cle primaire!")
        logger.error(separator)

        logger.error("Cle primaire dupliquee:")
        for pk in primary_keys:
            pk_lower = pk.lower()
            val = pk_values.get(pk_lower, pk_values.get(pk, "N/A"))
            logger.error(f"  {pk}: '{val}'")

        logger.error(f"{len(duplicates)} LIGNES EN DOUBLON TROUVEES:")
        logger.error("-" * 100)

        for i, row in enumerate(duplicates, 1):
            batch_idx = row.pop('_batch_index', 'N/A')
            logger.error(f"--- Doublon #{i} (index batch: {batch_idx}) ---")

            for col in columns:
                val = row.get(col, "N/A")
                val_str = str(val)[:60] + "..." if len(str(val)) > 60 else str(val)
                logger.error(f"  {col:<30}: {val_str}")

        if len(duplicates) >= 2:
            self._log_duplicates_comparison(duplicates, columns)

        logger.error(separator)
        logger.error("ACTION RECOMMANDEE:")
        logger.error("  1. Verifier les donnees source dans l'API AMUE")
        logger.error("  2. Contacter l'administrateur AMUE si les doublons ne devraient pas exister")
        logger.error("  3. Utiliser l'option UPSERT (import differentiel) pour ignorer les doublons")
        logger.error(separator)

    def log_conflict_details(
        self,
        table_name: str,
        columns: List[str],
        primary_keys: List[str],
        existing_row: Optional[Dict[str, Any]],
        new_row: Optional[Dict[str, Any]],
        pk_values: Dict[str, str]
    ) -> None:
        """
        Affiche les details d'un conflit entre ligne existante et nouvelle.

        Args:
            table_name: Nom de la table
            columns: Liste des colonnes
            primary_keys: Liste des cles primaires
            existing_row: Ligne existante en base
            new_row: Nouvelle ligne de l'API
            pk_values: Valeurs de la cle primaire en conflit
        """
        separator = "=" * 80
        logger.error(separator)
        logger.error(f"CONFLIT DE CLE PRIMAIRE - Table: {table_name}")
        logger.error(separator)

        logger.error("Cle primaire en conflit:")
        for pk in primary_keys:
            pk_lower = pk.lower()
            val = pk_values.get(pk_lower, pk_values.get(pk, "N/A"))
            logger.error(f"  {pk}: '{val}'")

        logger.error(f"{'COLONNE':<30} {'EXISTANTE (BD)':<40} {'NOUVELLE (API)':<40}")
        logger.error("-" * 110)

        for col in columns:
            existing_val = existing_row.get(col, "N/A") if existing_row else "N/A"
            new_val = new_row.get(col, "N/A") if new_row else "N/A"

            existing_str = str(existing_val)[:37] + "..." if len(str(existing_val)) > 40 else str(existing_val)
            new_str = str(new_val)[:37] + "..." if len(str(new_val)) > 40 else str(new_val)

            marker = " **" if str(existing_val) != str(new_val) else ""
            logger.error(f"  {col:<28} {existing_str:<40} {new_str:<40}{marker}")

        logger.error(separator)

        if existing_row and new_row:
            differences = [
                col for col in columns
                if str(existing_row.get(col, "")) != str(new_row.get(col, ""))
            ]

            if differences:
                logger.error(f"Colonnes differentes ({len(differences)}): {', '.join(differences)}")
            else:
                logger.error("Les deux lignes sont IDENTIQUES (doublon exact dans les donnees API)")

        logger.error(separator)

    # --- Methodes privees ---

    def _get_pk_indices(
        self,
        columns: List[str],
        primary_keys: List[str]
    ) -> List[tuple[str, int]]:
        """Retourne les indices des colonnes de cle primaire"""
        pk_indices = []
        for pk in primary_keys:
            pk_lower = pk.lower()
            if pk_lower in columns:
                pk_indices.append((pk_lower, columns.index(pk_lower)))
        return pk_indices

    def _build_pk_key(
        self,
        record: tuple,
        pk_indices: List[tuple[str, int]]
    ) -> tuple[str, Dict[str, str]]:
        """Construit la cle de PK et le dict des valeurs"""
        pk_key_parts = []
        pk_values = {}

        for pk_name, pk_idx in pk_indices:
            val = str(record[pk_idx]).strip() if record[pk_idx] is not None else ""
            pk_key_parts.append(f"{pk_name}={val}")
            pk_values[pk_name] = val

        return "|".join(pk_key_parts), pk_values

    def _matches_pk(
        self,
        record: tuple,
        pk_indices: List[tuple[str, int]],
        pk_values: Dict[str, str]
    ) -> bool:
        """Verifie si un record correspond aux valeurs de PK"""
        for pk_name, pk_idx in pk_indices:
            record_val = str(record[pk_idx]).strip() if record[pk_idx] is not None else ""
            expected_val = pk_values.get(pk_name, pk_values.get(pk_name.upper(), "")).strip()

            if record_val != expected_val:
                return False
        return True

    def _parse_pk_values(self, vals_str: str) -> List[str]:
        """Parse les valeurs de PK depuis une chaine (gere les virgules dans les valeurs)"""
        values = []
        current = ""
        in_quotes = False

        for char in vals_str:
            if char == "'" or char == '"':
                in_quotes = not in_quotes
            elif char == ',' and not in_quotes:
                values.append(current.strip())
                current = ""
                continue
            current += char

        if current:
            values.append(current.strip())

        return values

    def _log_duplicate_group(
        self,
        group_idx: int,
        duplicates: List[Dict[str, Any]],
        columns: List[str],
        primary_keys: List[str]
    ) -> None:
        """Log un groupe de doublons"""
        logger.error(f"{'='*50}")
        logger.error(f"GROUPE DE DOUBLONS #{group_idx} ({len(duplicates)} lignes)")
        logger.error(f"{'='*50}")

        if duplicates and '_pk_values' in duplicates[0]:
            pk_values = duplicates[0]['_pk_values']
            logger.error("Cle primaire dupliquee:")
            for pk in primary_keys:
                pk_lower = pk.lower()
                val = pk_values.get(pk_lower, "N/A")
                logger.error(f"  {pk}: '{val}'")

        self._log_duplicates_comparison(duplicates, columns)

    def _log_duplicates_comparison(
        self,
        duplicates: List[Dict[str, Any]],
        columns: List[str]
    ) -> None:
        """Log une comparaison entre doublons"""
        logger.error(f"Comparaison des {len(duplicates)} lignes:")
        logger.error("-" * 100)

        # En-tete
        header = f"{'COLONNE':<30}"
        for i in range(len(duplicates)):
            header += f" {'LIGNE #' + str(i+1):<25}"
        logger.error(header)
        logger.error("-" * 100)

        # Colonnes avec differences
        cols_with_diff = []
        for col in columns:
            values = [str(d.get(col, "")) for d in duplicates]
            row_str = f"  {col:<28}"

            for val in values:
                val_display = val[:22] + "..." if len(val) > 25 else val
                row_str += f" {val_display:<25}"

            if len(set(values)) > 1:
                row_str += " **DIFF**"
                cols_with_diff.append(col)

            logger.error(row_str)

        if cols_with_diff:
            logger.error(f"  -> Colonnes differentes: {', '.join(cols_with_diff)}")
        else:
            logger.error("  -> DOUBLONS IDENTIQUES (lignes 100% identiques)")
