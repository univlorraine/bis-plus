# amue/notifications/templates/base.py
"""Template de base pour les emails AMUE"""
from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseTemplate(ABC):
    """
    Template de base pour les emails

    Fournit les styles CSS communs et la structure HTML de base.
    Les templates spécifiques doivent implémenter render_content().
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

    @property
    @abstractmethod
    def header_color(self) -> str:
        """Couleur du header (gradient CSS)"""
        pass

    @abstractmethod
    def render_content(self, context: Dict[str, Any]) -> str:
        """
        Rendu du contenu principal de l'email

        Args:
            context: Données pour le template

        Returns:
            HTML du contenu
        """
        pass

    def render_header(self, title: str, subtitle: str = '') -> str:
        """Rendu du header"""
        subtitle_html = f'<div class="subtitle">{subtitle}</div>' if subtitle else ''
        return f"""
        <div class="header" style="background: {self.header_color};">
            <h1>{title}</h1>
            {subtitle_html}
        </div>
        """

    def render_footer(self, dag_id: str = '', airflow_url: str = 'http://localhost:8080') -> str:
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

    def render(self, context: Dict[str, Any]) -> str:
        """
        Rendu complet de l'email

        Args:
            context: Données pour le template

        Returns:
            HTML complet de l'email
        """
        title = context.get('title', 'Notification AMUE')
        subtitle = context.get('subtitle', '')
        dag_id = context.get('dag_id', '')

        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>{self.STYLES}</style>
        </head>
        <body>
            <div class="container">
                {self.render_header(title, subtitle)}
                <div class="content">
                    {self.render_content(context)}
                </div>
                {self.render_footer(dag_id)}
            </div>
        </body>
        </html>
        """

    @staticmethod
    def escape_html(text: str) -> str:
        """Échappe les caractères HTML"""
        if not text:
            return ''
        return (str(text)
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
                .replace('"', '&quot;')
                .replace("'", '&#x27;')
                .replace('\n', '<br>'))
