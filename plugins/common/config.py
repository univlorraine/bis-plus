# common/config.py
"""Constantes partagées entre les modules AMUE et ECC."""

#: Source protégée : les lignes _source='sifac_plus' ne sont jamais écrasées
#: par les imports ECC (guard WHERE dans le DO UPDATE SET).
PROTECTED_SOURCE: str = 'sifac_plus'
