# amue/notifications/templates.py
"""
Templates HTML unifies pour les notifications AMUE.

Ce module fournit tous les templates de notification via des methodes statiques
pour eviter l'instanciation multiple de classes.
"""
from typing import Any, Dict, List


class NotificationTemplates:
    """
    Templates HTML pour les notifications par email.

    Fournit des methodes statiques pour generer le HTML des emails
    de succes et d'erreur.
    """

    # Styles CSS communs
    STYLES = """
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            background: #fff;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        .header {
            padding: 30px;
            color: white;
        }
        .header h1 {
            margin: 0 0 10px 0;
            font-size: 24px;
            font-weight: 600;
        }
        .header .subtitle {
            opacity: 0.9;
            font-size: 14px;
        }
        .content {
            padding: 30px;
        }
        .info-grid {
            display: grid;
            grid-template-columns: 140px 1fr;
            gap: 12px;
            margin: 20px 0;
        }
        .info-label {
            font-weight: 600;
            color: #666;
        }
        .info-value {
            color: #333;
        }
        .message-box {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 4px;
            font-family: 'Courier New', monospace;
            font-size: 13px;
            overflow-x: auto;
            margin: 15px 0;
            border: 1px solid #e0e0e0;
            white-space: pre-wrap;
            word-break: break-word;
        }
        .footer {
            padding: 20px 30px;
            background: #f8f9fa;
            border-top: 1px solid #e0e0e0;
            font-size: 13px;
            color: #666;
        }
        .button {
            display: inline-block;
            padding: 12px 24px;
            text-decoration: none;
            border-radius: 4px;
            font-weight: 500;
            margin-top: 15px;
        }
        .button-primary {
            background: #2196F3;
            color: white;
        }
        code {
            background: #e8e8e8;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: 'Courier New', monospace;
            font-size: 13px;
        }
        .badge {
            display: inline-block;
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 600;
            text-transform: uppercase;
        }
        .badge-success { background: #e8f5e9; color: #2e7d32; }
        .badge-error { background: #ffebee; color: #c62828; }
        .badge-warning { background: #fff3e0; color: #ef6c00; }
    """

    # Couleurs de header
    HEADER_COLOR_ERROR = "linear-gradient(135deg, #f44336 0%, #d32f2f 100%)"
    HEADER_COLOR_SUCCESS = "linear-gradient(135deg, #4CAF50 0%, #388E3C 100%)"

    @staticmethod
    def escape_html(text: str) -> str:
        """Echappe les caracteres HTML"""
        if not text:
            return ''
        return (str(text)
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&#x27;')
                .replace('\n', '<br>'))

    @classmethod
    def _render_header(cls, title: str, subtitle: str, header_color: str) -> str:
        """Rendu du header"""
        subtitle_html = f'<div class="subtitle">{subtitle}</div>' if subtitle else ''
        return f"""
        <div class="header" style="background: {header_color};">
            <h1>{title}</h1>
            {subtitle_html}
        </div>
        """

    @classmethod
    def _render_footer(cls, dag_id: str = '', airflow_url: str = 'http://localhost:8080') -> str:
        """Rendu du footer"""
        button_html = ''
        if dag_id:
            button_html = f'''
            <a href="{airflow_url}/dags/{dag_id}" class="button button-primary">
                Voir dans Airflow
            </a>
            '''

        return f"""
        <div class="footer">
            <p>
                <strong>Actions recommandees:</strong><br>
                - Consultez les logs Airflow pour plus de details<br>
                - Verifiez la configuration des variables Airflow<br>
                - Contactez l'equipe de support si le probleme persiste
            </p>
            {button_html}
        </div>
        """

    @classmethod
    def _wrap_html(cls, header: str, content: str, footer: str) -> str:
        """Enveloppe le contenu dans le template HTML de base"""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>{cls.STYLES}</style>
        </head>
        <body>
            <div class="container">
                {header}
                <div class="content">
                    {content}
                </div>
                {footer}
            </div>
        </body>
        </html>
        """

    @classmethod
    def render_error(cls, context: Dict[str, Any]) -> str:
        """
        Genere le HTML pour une notification d'erreur.

        Args:
            context: Dictionnaire contenant:
                - title: Titre de l'email
                - subtitle: Sous-titre (date)
                - dag_id: ID du DAG
                - task_id: ID de la tache
                - error_type: Type d'erreur
                - error_message: Message d'erreur
                - status: Statut (failed)

        Returns:
            HTML complet de l'email
        """
        title = context.get('title', 'Erreur d\'Import AMUE')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        task_id = context.get('task_id', 'unknown')
        error_type = context.get('error_type', 'UnknownError')
        error_message = cls.escape_html(context.get('error_message', ''))
        status = context.get('status', 'failed')

        content = f"""
        <div style="background: #ffebee; border-left: 4px solid #f44336; padding: 20px; border-radius: 4px;">
            <h2 style="color: #c62828; margin-top: 0; font-size: 18px;">
                Erreur Detectee
            </h2>

            <div class="info-grid">
                <div class="info-label">DAG:</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Tache:</div>
                <div class="info-value"><strong>{task_id}</strong></div>

                <div class="info-label">Type d'erreur:</div>
                <div class="info-value"><code>{error_type}</code></div>

                <div class="info-label">Statut:</div>
                <div class="info-value">
                    <span class="badge badge-error">{status}</span>
                </div>
            </div>

            <div style="margin-top: 20px;">
                <strong>Message d'erreur:</strong>
                <div class="message-box">{error_message}</div>
            </div>
        </div>
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_ERROR)
        footer = cls._render_footer(dag_id)

        return cls._wrap_html(header, content, footer)

    @classmethod
    def render_success(cls, context: Dict[str, Any]) -> str:
        """
        Genere le HTML pour une notification de succes.

        Args:
            context: Dictionnaire contenant:
                - title: Titre de l'email
                - subtitle: Sous-titre (date)
                - dag_id: ID du DAG
                - execution_date: Date d'execution
                - duration: Duree d'execution
                - tables_imported: Liste des tables importees
                - total_rows: Nombre total de lignes inserees
                - total_fetched: Nombre total de lignes recuperees

        Returns:
            HTML complet de l'email
        """
        title = context.get('title', 'Import AMUE Reussi')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', 'unknown')
        execution_date = context.get('execution_date', '')
        duration = context.get('duration', 'N/A')
        tables_imported = context.get('tables_imported', [])
        total_rows = context.get('total_rows', 0)
        total_fetched = context.get('total_fetched', total_rows)
        total_updated = context.get('total_updated', 0)

        # Rendu de la liste des tables
        tables_html = cls._render_tables_list(tables_imported)

        content = f"""
        <div style="background: #e8f5e9; border-left: 4px solid #4CAF50; padding: 20px; border-radius: 4px;">
            <h2 style="color: #2e7d32; margin-top: 0; font-size: 18px;">
                Import Termine avec Succes
            </h2>

            <div class="info-grid">
                <div class="info-label">DAG:</div>
                <div class="info-value"><strong>{dag_id}</strong></div>

                <div class="info-label">Date:</div>
                <div class="info-value">{execution_date}</div>

                <div class="info-label">Duree:</div>
                <div class="info-value"><strong>{duration}</strong></div>

                <div class="info-label">Tables:</div>
                <div class="info-value"><strong>{len(tables_imported)}</strong> table(s)</div>

                <div class="info-label">Lignes API:</div>
                <div class="info-value"><strong>{total_fetched:,}</strong> ligne(s) recuperee(s)</div>

                <div class="info-label">Lignes DB:</div>
                <div class="info-value"><strong>{total_rows:,}</strong> ligne(s) inseree(s)</div>

                <div class="info-label">Mises a jour:</div>
                <div class="info-value"><strong>{total_updated:,}</strong> ligne(s) mise(s) a jour</div>

                <div class="info-label">Statut:</div>
                <div class="info-value">
                    <span class="badge badge-success">succes</span>
                </div>
            </div>
        </div>

        {tables_html}
        """

        header = cls._render_header(title, subtitle, cls.HEADER_COLOR_SUCCESS)
        footer = cls._render_footer(dag_id)

        return cls._wrap_html(header, content, footer)

    @classmethod
    def _render_tables_list(cls, tables: List[Dict[str, Any]]) -> str:
        """Rendu de la liste des tables importees"""
        if not tables:
            return ''

        rows_html = ''
        for table in tables:
            name = table.get('table_name', table.get('name', 'unknown'))
            rows_fetched = table.get('rows_fetched', 0)
            rows_inserted = table.get('rows_inserted', table.get('rows', 0))
            rows_updated = table.get('rows_updated', 0)
            status = table.get('status', 'success')
            import_type = table.get('import_type', 'full')

            badge_class = 'badge-success' if status == 'success' else 'badge-error'
            rows_html += f"""
            <tr>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0;">
                    <strong>{name}</strong>
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: right;">
                    {rows_fetched:,}
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: right;">
                    {rows_inserted:,}
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: right;">
                    {rows_updated:,}
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: center;">
                    {import_type}
                </td>
                <td style="padding: 10px; border-bottom: 1px solid #e0e0e0; text-align: center;">
                    <span class="badge {badge_class}">{status}</span>
                </td>
            </tr>
            """

        return f"""
        <div style="margin-top: 25px;">
            <h3 style="color: #333; font-size: 16px; margin-bottom: 15px;">
                Detail des tables importees
            </h3>
            <table style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="background: #f5f5f5;">
                        <th style="padding: 12px; text-align: left; border-bottom: 2px solid #e0e0e0;">Table</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #e0e0e0;">Recuperees</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #e0e0e0;">Inserees</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #e0e0e0;">MAJ</th>
                        <th style="padding: 12px; text-align: center; border-bottom: 2px solid #e0e0e0;">Type</th>
                        <th style="padding: 12px; text-align: center; border-bottom: 2px solid #e0e0e0;">Statut</th>
                    </tr>
                </thead>
                <tbody>
                    {rows_html}
                </tbody>
            </table>
        </div>
        """
