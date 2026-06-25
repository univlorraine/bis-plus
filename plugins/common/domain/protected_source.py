# common/domain/protected_source.py
"""Layer: domain

Règle métier partagée AMUE/ECC : quelle source de données ne doit jamais être
écrasée par un import concurrent."""

#: Source protégée : les lignes _source='sifac_plus' ne sont jamais écrasées
#: par les imports ECC (guard WHERE dans le DO UPDATE SET).
PROTECTED_SOURCE: str = 'sifac_plus'
