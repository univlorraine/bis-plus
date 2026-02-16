"""
Services AMUE - Logique métier et orchestration

Sous-packages :
    - bluegreen : Gestion blue/green (état, switch, sync, rollback)
    - api : Interaction API (polling, statut)

Modules standalone :
    - metadata_manager : Gestion des métadonnées d'import
    - retry_service : Gestion des retries avec backoff

Note: Les imports sont effectués à la demande pour éviter les imports circulaires.
Utilisez les imports directs depuis les sous-modules.
"""
