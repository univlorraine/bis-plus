"""
Services AMUE - Logique métier et orchestration

Ce module regroupe les services de haut niveau :
    - metadata_manager : Gestion des métadonnées d'import
    - polling_service : Attente de disponibilité API
    - retry_service : Gestion des retries avec backoff
    - status_checker : Vérification du statut des tables
    - bluegreen_manager : Gestion de l'état blue/green
    - view_switcher : Switch atomique des vues
    - schema_synchronizer : Synchronisation des schémas
    - rollback_manager : Rollback vers l'état précédent

Note: Les imports sont effectués à la demande pour éviter les imports circulaires.
Utilisez les imports directs depuis les sous-modules.
"""