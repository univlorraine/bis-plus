"""
Styles CSS et helpers HTML partagés par tous les templates de notification.

Module générique (AMUE / ECC) : ne contient aucune logique métier liée à
un plugin. Les classes spécifiques (ErrorTemplates AMUE, ECCNotificationTemplates)
en héritent.
"""


class BaseTemplates:
    """
    Classe de base avec les styles CSS et les composants HTML réutilisables.

    Toutes les classes de templates héritent de cette classe pour accéder
    aux styles et aux méthodes _render_*.
    """

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
            grid-template-columns: 160px 1fr;
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
        .badge-error   { background: #ffebee; color: #c62828; }
        .badge-warning { background: #fff3e0; color: #ef6c00; }
        .badge-info    { background: #e3f2fd; color: #1565c0; }
    """

    # Couleurs des headers
    HEADER_COLOR_ERROR   = "linear-gradient(135deg, #f44336 0%, #d32f2f 100%)"
    HEADER_COLOR_SUCCESS = "linear-gradient(135deg, #4CAF50 0%, #388E3C 100%)"
    HEADER_COLOR_SYNC    = "linear-gradient(135deg, #2196F3 0%, #1565C0 100%)"
    HEADER_COLOR_ROLLBACK = "linear-gradient(135deg, #FF9800 0%, #F57C00 100%)"
    HEADER_COLOR_WARNING = "linear-gradient(135deg, #FF9800 0%, #F57C00 100%)"

    @staticmethod
    def escape_html(text: str) -> str:
        """Échappe les caractères HTML dangereux."""
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
        """Rendu du header commun."""
        subtitle_html = f'<div class="subtitle">{subtitle}</div>' if subtitle else ''
        return f"""
        <div class="header" style="background: {header_color};">
            <h1>{title}</h1>
            {subtitle_html}
        </div>
        """

    @classmethod
    def _render_footer(
        cls,
        show_actions: bool = False,
        actions: str = '',
    ) -> str:
        """
        Rendu du footer commun.

        Args:
            show_actions: Affiche le bloc "Actions recommandées" si True.
            actions: Contenu HTML des actions (lignes séparées par <br>).
        """
        actions_html = ''
        if show_actions:
            actions_content = actions or (
                "- Consultez les logs Airflow pour plus de détails<br>"
                "- Vérifiez la configuration des variables Airflow<br>"
                "- Contactez l'équipe de support si le problème persiste"
            )
            actions_html = f"""
            <p>
                <strong>Actions recommandées :</strong><br>
                {actions_content}
            </p>
            """

        return f"""
        <div class="footer">
            {actions_html}
        </div>
        """

    @classmethod
    def _render_stacktrace(cls, traceback_str: 'str | None') -> str:
        """Stack traces exclues des emails pour éviter l'exposition d'informations système.
        Consulter les logs Airflow pour les détails techniques."""
        return ''

    @classmethod
    def _wrap_html(cls, header: str, content: str, footer: str) -> str:
        """Enveloppe le contenu dans le template HTML de base."""
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
