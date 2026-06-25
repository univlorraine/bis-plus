Cette API permet les appels à la base tampon de type PostgreSQL.

Fournit l'accès aux informations financières de Sifac+ (par SAP CDS Views) pour remplir un Système d'Information Décisionnel (SID) d'un établissement en utilisant des APIs d'extraction de données.,
    
Les tables de la base de donnée Sifac sont recopiées dans une base temporaire mise à jour quotidiennement par la technique des CDS Views (certaines sont des jointures entre plusieurs tables SAP).

Les flux d'extraction sont des requêtes HTTPS de type REST envoyées par le serveur PostgreSQL par l'intermédiaire d'un API Manager.

La colonne ***Delta*** indique le nom des champs de type date pouvant être utilisés dans les paramètres de l'URL pour filtrer les lignes de la base de données.

Certaines tables, comme BSEG, n'ont pas de colonne date permettant le mode delta mais elles sont reliées à d'autres tables (qui sont leurs entêtes) ayant une colonne date et par jointure, leur lignes sont accessibles.

| Table            | Description                                                    | Delta              | 
|------------------|----------------------------------------------------------------|--------------------| 
| A003             | Code taxe                                                      |                    | 
| A998             | Sté                                                            |                    | 
| ACDOCA           | Journal universel                                              | timestamp          | 
| ADCP             | Affectation personne/adresse (gestion centrale des adresses)   |                    | 
| ADR6             | Adresses e-mail (Business Address Services)                    |                    | 
| ADRC             | Adresses (Business Address Services)                           |                    | 
| ADRP             | Personnes (gestion centrale des adresses)                      |                    | 
| ADRPS            | Table antémémoire : personnes (gestion centrale adresses)      |                    | 
| AGR_1016         | Nom du profil du groupe d'activités                            |                    | 
| AGR_1250         | Données d'autorisation pour le groupe d'activité               |                    | 
| AGR_1251         | Données d'autorisation pour le groupe d'activité               |                    | 
| AGR_1252         | Niveau d'organisation pour les autorisations                   |                    | 
| AGR_AGRS         | Rôle composite pour rôles individuels                          |                    | 
| AGR_DEFINE       | Définition des rôles                                           |                    | 
| AGR_TCODES       | Affectation des rôles aux codes de transaction                 |                    | 
| AGR_TEXTS        | Structure de classement pour hiérarchie de menus - client      |                    | 
| AGR_USERS        | Affectation des rôles aux utilisateurs                         |                    | 
| ANEP             | Postes individuels d'immobilisation                            |                    | 
| ANLA             | Segment des fiches d'immobilisation                            |                    | 
| ANLB             | Paramètres d'amortissement                                     |                    | 
| ANLC             | Zones valeurs d'immobilisations                                |                    | 
| ANLZ             | Imputations d'immobilisat. avec délai de validité              |                    | 
| AUFK             | Données de base ordre                                          |                    | 
| AUTHX            | Zones des autorisations (gestion avec SU20)                    |                    | 
| BKPF             | En-tête pièce pour comptabilité                                | cpudt              | 
| BNKA             | Données bancaires de base                                      | erdat              | 
| BPJA             | Enreg.totaux pr valeur totale annuelle Support contrôle        |                    | 
| BSAD             | Index secondaire comptable pour clients (postes rapprochés)    | cpudt              | 
| BSAK             | FI : index secondaire pour fournisseurs (postes rapprochés)    | cpudt              | 
| BSAS             | FI : index secondaire pour comptes généraux (postes rappr.)    | cpudt              | 
| BSEG             | Segment de pièce comptabilité                                  | cpudt de bkpf      | 
| BSID             | Index secondaire comptable pour clients                        | cpudt              | 
| BSIK             | Index secondaire comptable pour fournisseurs                   | cpudt              | 
| BSIS             | Comptabilité : index secondaire pr comptes généraux            | cpudt de bkpf      | 
| BUKF_KFDSRC      | Ratios - relation ratio/sources de données                     |                    | 
| BUKF_KF_T        | Ratio - texte pour le ratio                                    |                    | 
| BUT000           | Données générales BP                                           |                    | 
| CCCFLOW          | Client Copy Control Flow                                       |                    | 
| CEPC             | Table des données de base de centres de profit                 |                    | 
| COBK             | Objet CO : En-tête de pièce                                    |                    | 
| COBRB            | Règles de répartit. des caractérist. d'imput. (imput. ordre)   |                    | 
| COEP             | Objet CO: Postes individuels liés à la période                 |                    | 
| COSP             | CO Object: Cost Totals for External Postings                   |                    | 
| COSP_BAK         | CO Object: Cost Totals for External Postings                   |                    | 
| COSS             | CO Object: Cost Totals for Internal Postings                   |                    | 
| COSS_BAK         | CO Object: Cost Totals for Internal Postings                   |                    | 
| COVJ             | Objet CO : postes individuels par exercice et en-tête pièce    |                    | 
| COVP             | Tables générées pour une vue                                   |                    | 
| CSKS             | Fiche centres                                                  |                    | 
| DD07T            | DD : textes de constantes de domaine (multilingues)            |                    | 
| DD07V            | Generated Table for View DD07V      INDEX NOT UNIQUE !!!!!     |                    | 
| DD27S            | Structure des cdsviews                                         |                    | 
| DFKKBPTAXNUM     | Identifications fiscales                                       |                    | 
| EKAB             | Documentation des appels sur contrat                           | aedat              | 
| EKBE             | Historique du document d'achat                                 | cpudt              | 
| EKBE_MA          | History of Purchasing Document at Account Assignment Level     |                    | 
| EKET             | Echéances du programme de livraison                            | bedat              | 
| EKKN             | Imputation dans document d'achat                               | aedat de ekpo      | 
| EKKO             | En-tête document d'achat                                       | aedat              | 
| EKPA             | Rôles partenaire dans les Achats                               | aedat              | 
| EKPO             | Poste document d'achat                                         |                    | 
| FAGL_SPLINFO     | Info de répartition postes non soldés                          | cpudt              | 
| FM01             | Périmètres financiers                                          |                    | 
| FM01H            | Affect. dép. exercice de variantes de hiérarchie à périmFin.   |                    | 
| FMBDT            | Table de totaux budget CB                                      |                    | 
| FMBH             | En-tête budget comptab. budgét. (pièces de saisie)             | crtdate            | 
| FMBL             | Comptabil. budgét. Lignes de pièce budgét. (pces de saisie)    | crtdate de fmbh    | 
| FMBUDTYPET       | Textes pour la définition du type de budget                    |                    | 
| FMCI             | Comptes budgétaires données de base                            |                    | 
| FMFCTR           | Fiche du centre financier                                      |                    | 
| FMFINCODE        | code de financement                                            |                    | 
| FMGLFLEXA        | Grand livre : postes individuels au réel                       | timestamp          | 
| FMGLFLEXT        | Grand livre Secteur public : totaux     INDEX NOT UNIQUE !!!!! |                    | 
| FMHICI           | Hiérarchie des comptes budgétaires                             |                    | 
| FMHISV           | Table de hiérarchie des centres financ. dans variante hiér.    |                    | 
| FMIA             | Table postes indiv. 'réel' pour comptabilité budgétaire        | cpudt              | 
| FMIFIHD          | Table en-têtes FI comptabilité budgétaire                      |                    | 
| FMIFIIT          | Table des postes indiv. FI Comptabilité budgétaire             | psobt              | 
| FMIOI            | Pièces d'engagement compt. budg.                               | cpudt              | 
| FMIT             | Table de totaux pr la comptabilité budg.                       |                    | 
| FMKF_TERM        | Ratios - termes CB                                             |                    | 
| FMKF_TERM_RB     | Ratios - termes CB                                             |                    | 
| FMMEASURE        | Mesure FM données de base                                      |                    | 
| FMTA_DEP_D       |                                                                |                    | 
| FMTA_RBQ         |                                                                |                    | 
| FMTA_REC_D       |                                                                |                    | 
| FMZUOB           | Affectation d'un objet CO à une adresse budgétaire             |                    | 
| GLPCA            | EC-PCA : postes individuels au réel                            | cpudt              | 
| KBLK             | Réservations de crédit en-tête                                 |                    | 
| KNA1             | Fiche client (partie générale)                                 |                    | 
| KNB1             | Fiche client (société)                                         |                    | 
| KNBK             | Fiche client (coord.banc.)                                     |                    | 
| KNVI             | Fiche client: Indicateurs taxe                                 |                    | 
| KNVP             | Base de données clients : rôles partenaire                     |                    | 
| KNVV             | Fiche client : données ventes                                  |                    | 
| KONH             | Conditions (en-tête)                                           |                    | 
| KONP             | Conditions (poste)                                             |                    | 
| LFA1             | Base fournisseurs (généralités)                                |                    | 
| LFB1             | Base fournisseurs (société)                                    |                    | 
| LFBK             | Base de données fournisseurs (coordonnées bancaires)           |                    | 
| MAKT             | Désignations des articles                                      |                    | 
| MARA             | Données article générales                                      |                    | 
| MARC             | Données division de l'article                                  |                    | 
| MLAN             | Classification fiscale de l'article                            |                    | 
| MLST             | Jalon                                                          |                    | 
| MLTX             | Désignation de jalon                                           |                    | 
| MVKE             | Données commerciales de l'article                              |                    | 
| NAST             | Bons de commandes                                              | erdat              | 
| PA0001           | Infotype 0001 (Affectation)                                    |                    | 
| PA0002           | Fiche du personnel infotype 0002 (identité)                    |                    | 
| PA0006           | Fiche du personnel infotype 0006 (adresses)                    |                    | 
| PA0009           | Fiche du personnel infotype 0009 (coordonnées bancaires)       |                    | 
| PA0017           | Fiche du personnel Infotype 0017 (Rég. indem. frais dépl.)     |                    | 
| PA0105           | Infotype 0105 (Communications)                                 |                    | 
| PROJ             | Définition de projet                                           |                    | 
| PRPS             | Elément d'OTP - données de base                                |                    | 
| PRPS_RH          | Convention - RH                                                |                    | 
| PSMFPTYPET       | Table des textes pour table du Customizing PSMFPTYPE           |                    | 
| PTRV_CHANGE      | Infos sur la création des missions                             | laufd              | 
| PTRV_COMM_AMT    | Données détaillées : transf. ComptaBudg lors sauveg. déplac.   |                    | 
| PTRV_COMM_ITM    | Données d'en tête : transf. ComptaBudg lors sauveg. déplac.    | dates de ptrv_head | 
| PTRV_DOC_IT      | Transfert dépl. -> FI/CO : lignes justif. interm. dépl.        |                    | 
| PTRV_HEAD        | Données d'un déplacement                                       | dates              | 
| PTRV_KREDP       | Détermination de fournisseur via matricule indiqué             |                    | 
| PTRV_PERIO       | Données période d'un déplacement                               | dates de ptrv_head | 
| PTRV_ROT_AWKEY   | Affectation résultats dépl. à n° et ligne document comptable   |                    | 
| PTRV_SCOS        | Statistiques dépl. - affectation coûts                         | dates de ptrv_head | 
| PTRV_SHDR        | Statistiques déplacement - Montants déplacement                | chngdate           | 
| PTRV_SREC        | Statistiques déplacement - Justificatifs                       | rec_date           | 
| RBCO             | Poste de document - facture fournisseur - imputation           |                    | 
| RBKP             | En-tête facture fournisseur                                    | cpudt              | 
| RSEG             | Poste de document - entrée de facture                          | cpudt de rkbp      | 
| SETHEADERT       | Désignation synthétique des sets                               |                    | 
| SETLEAF          | Valeurs dans sets                                              |                    | 
| SETNODE          | Sets subordonnés dans sets                                     |                    | 
| SKA1             | Base comptes généraux (plan comptable)                         |                    | 
| SKAT             | Fiche cpte gén. (plan cptble : désignation)                    |                    | 
| SKB1             | Base comptes généraux (société)                                |                    | 
| SRGBTBREL        | Lien métadonnée et PJ                                          |                    | 
| T000             | Mandants                                                       |                    | 
| T001             | Sociétés                                                       |                    | 
| T003             | Types de pièces                                                |                    | 
| T005T            | Désignation des pays                                           |                    | 
| T007A            | Code TVA                                                       |                    | 
| T007V            | Codes TVA à transférer                                         |                    | 
| T023             | Groupes de marchandises                                        |                    | 
| T023T            | Désignations des groupes de marchandises                       |                    | 
| T024             | Groupes d'acheteurs                                            |                    | 
| T134T            | Désignations pour types d'article                              |                    | 
| T161             | Types de documents d'achat                                     |                    | 
| T161T            | Désignations des types de documents d'achat                    |                    | 
| T163B            | Types d'historique de commande                                 |                    | 
| T163C            | Textes des types d'historique de commande                      |                    | 
| T433T            | Textes des jalons                                              |                    | 
| T499S            | Localisation                                                   |                    | 
| T501T            | Désignations des catégories de salariés                        |                    | 
| T503T            | Désignations des statuts de salariés                           |                    | 
| T706B5           | Désignations des catégories de frais de dépl.                  |                    | 
| T706T            | Désignations du schéma de déplacement                          |                    | 
| T881T            | Texte ledger FI-SL                                             |                    | 
| TCJ1T            | Types de projets                                               |                    | 
| TCURR            | Taux de conversion                                             |                    | 
| TCURX            | Décimales des devises                                          |                    | 
| TFKB             | Domaines fonctionnels                                          |                    | 
| TGSB             | Domaines d'activités                                           |                    | 
| TIBAN            | IBAN                                                           |                    | 
| TJ01T            | Textes des opérations commerciales                             |                    | 
| TKVST            | Libellés des versions CO                                       |                    | 
| TSKDT            | Taxes : Clients textes                                         |                    | 
| TSKMT            | Impôts: articles : textes                                      |                    | 
| TSPAT            | Entité organisationnelle : secteurs d'activité ADV : textes    |                    | 
| TSTCT            | Libellés des codes transaction                                 |                    | 
| TTYPT            | Désignation des types d'objets pour la comptabilité            |                    | 
| TVAKT            | Documents de vente: Types: Textes                              |                    | 
| TVAUT            | Documents commerciaux: raisons de la commande: textes          |                    | 
| TVKMT            | Articles : Groupes de natures comptables: Textes               |                    | 
| TVKO             | Entité organisationnelle : organisations commerciales          |                    | 
| TVKOT            | Entité organisation. : organisations commerciales : textes     |                    | 
| TVTWT            | Entité organisationnelle : canaux de distribution : textes     |                    | 
| TX_CJI3          | Projets : postes indiv. coûts réels                            |                    | 
| USR01            | Fiche utilisateur (données d'exécution)                        |                    | 
| USR02            | Données de connexion (utilisation côté noyau !!!)              |                    | 
| USR21            | Affectation nom utilisateur clé d'adresse                      |                    | 
| UST12            | Fiche utilisateur : autorisations                              |                    | 
| VBAK             | Document commercial: données d'en-tête                         | erdat              | 
| VBAP             | Document commercial: données du poste                          | erdat, aedat       | 
| VBFA             | Flux de documents commerciaux                                  |                    | 
| VBKD             | Document commercial: données commerciales                      |                    | 
| VBRK             | Entete facture SD                                              | erdat, aedat       | 
| VBRP             | Poste facture SD                                               | erdat              | 
| VBUP             | Document commercial: statuts de poste                          |                    | 
| V_FMED           | Generierte Tabelle zu einem View                               |                    | 
| ZMAR_ENVELOP     | Marchés : Table spécifique pour les enveloppes                 |                    | 
| ZMAR_UFO         | Marché : Unités fonctionnelles ou opération                    |                    | 
| ZSIFACMMTA_CTVA  | SIFAC : Secteur d'activité / Codes TVA                         |                    | 
| ZSIFACTAVALIDEUR | Table des responsables, suppleants et des assistantes          |                    | 
| ZSIFACTA_FDVDETL |                                                                |                    | 
| ZSIFACTA_SUIVFDV | Table de suivi Interface FCA Ventes Externes                   |                    | 
| ZSTA_CYCLE_D     | Table historique des Statuts des factures Chorus Pro           |                    | 
| ZSTA_ENTETEFAC_D | Données d'entête de facture Demat CPP                          |                    | 
 
 
Correspondance des appels selon la base tampon : 
 
| Action                   | SQLite                                                | PostgreSQL                               | HEADER                                | 
|--------------------------|-------------------------------------------------------|------------------------------------------|---------------------------------------| 
| Récupération pagination  | /table?nom=\${TABLE}&f=json&top=1                     | /rpc/info_pagination?nom_table=\${TABLE} |                                       | 
| Récupération table       | /table?nom=\${TABLE}&f=json&top=\${TOP}&skip=\${SKIP} | /\${TABLE}?limit=\${TOP}&offset=\${SKIP} |                                       | 
| Récupération status      | /admin?status                                         | /rpc/get_status                          | Accept : text/plain                   | 
| Récupération description | /table?nom=\${TABLE}&f=json&top=1&desc                | /rpc/get_file?name=\${TABLE}.def         | Accept : text/plain                   | 
| Récupération archive csv | /admin?get=\${TABLE}.gz                               | /rpc/get_file_gz?name=\${TABLE}.gz       | Accept : application/gz               | 
| Récupération fichier csv | /admin?get=\${TABLE}.csv                              | /rpc/get_file?name=\${TABLE}.csv         | Accept : text/plain  (pas text/csv !) | 
| Récupération clés        | /admin?get=\${TABLE}.keys                             | /rpc/get_file?name=\${TABLE}.keys        | Accept : text/plain                   | 
 
Pour avoir le résultat de la dernière extraction (quotidienne), utiliser l'API ***Récupération status*** qui renvoie un flux json contenant pour chaque table (name) le nombre de lignes (count) extraites, la taille d'une ligne (row_size),... (voir schéma Status) 
 
Certaines tables ont des erreurs (champ msg) mais si le champ status=OK, ne pas en tenir compte. 
 
Chaque table ayant le ***status=OK*** est récupérable avec la requête ***Récupération table*** avec 
- pour SQLite ***top=\${TOP}*** nombre de lignes à récupérer, constant par table, ***skip=\${SKIP}*** nombre de lignes à sauter (= top+skip), 
- pour PostgreSQL ***limit=\${TOP}*** nombre de lignes à récupérer, constant par table, ***offset=\${SKIP}*** nombre de lignes à sauter (= top+skip). 
 
Pour connaître le nombre ***TOP***, appeler l'url ***Récupération pagination*** qui retournera le nombre de lignes et le nombre de pages, contenant chacune ***top*** lignes. (voir schéma pagination_json) 
 
Par ex, pour la table csks, si  ***count*** retourne 321, ***top*** retourne 99 et ***pages*** retourne 4, il faut appeler 4 fois l'url avec ***top=99*** et skip incrémenté de top à chaque appel, sauf la première fois ou il devra être absent, ex pour csks : 
 
| SQLite                                                                                 | PostgreSQL                                                                       | 
|----------------------------------------------------------------------------------------|----------------------------------------------------------------------------------| 
| https://api.amue.fr/finances/cdv/v1/preprod/ETAB/table?nom=csks&top=99&f=json          | https://api.amue.fr/finances/cdv/v1/preprod/ETAB/csks&limit=99&f=json            | 
| https://api.amue.fr/finances/cdv/v1/preprod/ETAB/table?nom=csks&top=99&skip=99&f=json  | https://api.amue.fr/finances/cdv/v1/preprod/ETAB/csks&limit=99&offset=99&f=json  | 
| https://api.amue.fr/finances/cdv/v1/preprod/ETAB/table?nom=csks&top=99&skip=198&f=json | https://api.amue.fr/finances/cdv/v1/preprod/ETAB/csks&limit=99&offset=198&f=json | 
| https://api.amue.fr/finances/cdv/v1/preprod/ETAB/table?nom=csks&top=99&skip=297&f=json | https://api.amue.fr/finances/cdv/v1/preprod/ETAB/csks&limit=99&offset=297&f=json | 
 
La dernière requête ramènera 321 (=count) - 297 lignes. 
 
 
En mode DELTA, pour connaître le nombre de lignes à récupérer, utiliser le filtre dans l’URL comme suit : 

| SQLite                                                                                                                                                                                                                                                                                                                         | PostgreSQL                                                                                                                                                                                                                                                                                                                                                                                                                                | 
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| <code>/table?nom=\${table}&top=1&q=colonne op valeur</code>                                                                                                                                                                                                                                                                    | <code>/\${table}?colonne=op.valeur</code>                                                                                                                                                                                                                                                                                                                                                                                                 | 
| <table>  <thead>  <tr>  <th>Opérateur 'op'</th>  </tr>  </thead>  <tbody>  <tr> <td><code>>=</code></td>  </tr>  <tr>  <td><code>></code></td> </tr>  <tr>  <td><code>></code></td>  </tr> <tr> <td><code><</code></td>  </tr>  <tr>  <td><code><=</code></td>  </tr> <tr>  <td><code>!=</code></td>  </tr> </tbody>  </table> | <table>  <thead>  <tr>  <th>Opérateur 'op'</th>  <th>Signification</th>  </tr>  </thead>  <tbody>  <tr>  <td>gte</td>  <td><code>>=</code></td>  </tr>  <tr>  <td>gt</td>  <td><code>></code></td> </tr>  <tr>  <td>eq</td>  <td><code>=</code></td>  </tr> <tr>  <td>lt</td>  <td><code><</code></td>  </tr>  <tr>  <td>lte</td>  <td><code><=</code></td>  </tr> <tr>  <td>neq</td>  <td><code>!=</code></td>  </tr> </tbody>  </table> | 
| ex <code>/table?nom=csks?q=datab >= 20250101</code>                                                                                                                                                                                                                                                                            | ex <code>/csks?datab=gte.20250101</code>                                                                                                                                                                                                                                                                                                                                                                                                  | 
|                                                                                                                                                                                                                                                                                                                                | avec les headers HTTP                                                                                                                                                                                                                                                                                                                                                                                                                     | 
|                                                                                                                                                                                                                                                                                                                                | <code>Prefer: count=exact</code>                                                                                                                                                                                                                                                                                                                                                                                                          | 
|                                                                                                                                                                                                                                                                                                                                | <code>Range-Unit: items</code>                                                                                                                                                                                                                                                                                                                                                                                                            | 
|                                                                                                                                                                                                                                                                                                                                | <code>Range: 0-1</code>                                                                                                                                                                                                                                                                                                                                                                                                                   | 
| En réponse, le code retour HTTP = <code>200 OK</code>  avec dans le corps du message, le champ count contenant le nombre de lignes                                                                                                                                                                                             | En réponse le code retour HTTP =  <code>206 Partial content</code> avec le header de la réponse contenant le nombre de lignes <code>Content-Range : 0-1/1696</code>                                                                                                                                                                                                                                                                       | 