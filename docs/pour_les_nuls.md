# Le projet expliqué simplement — *Pour les nuls*

Ce document explique le projet **sans jargon**, comme si vous n'aviez jamais fait de data ou de code. Si un mot technique apparaît, il est expliqué juste après. Pour la version technique complète, voir [`doc.md`](doc.md).

---

## 1. C'est quoi, en une phrase ?

> On récupère automatiquement des données sur des **films** (via un site qui s'appelle TMDB), on les **nettoie**, on les **range proprement**, et on les transforme en **tableaux prêts à être analysés** — le tout sans intervention manuelle.

C'est ce qu'on appelle une **pipeline de données** : une chaîne automatisée qui prend des données brutes d'un côté et produit des données propres et exploitables de l'autre.

---

## 2. L'analogie : un restaurant

Le projet fonctionne **exactement comme la cuisine d'un restaurant**. C'est l'image à garder en tête tout du long.

| Dans un restaurant | Dans le projet | Nom technique |
|---|---|---|
| Le marché / le fournisseur | Le site TMDB qui fournit les infos sur les films | **API TMDB** |
| Aller chercher les ingrédients | Le programme qui télécharge les données | **Ingestion** |
| Le garde-manger / le frigo | L'endroit où on stocke les données brutes | **Data Lake (MinIO)** |
| Laver, éplucher, trier | Nettoyer les données | **Spark Staging** |
| Précuire, assembler | Enrichir et croiser les données | **Spark Curated** |
| La grande cuisine pro | La base de données puissante | **Snowflake** |
| Dresser les assiettes (recettes) | Mettre en forme pour l'analyse | **DBT** |
| Les plats servis au client | Les tableaux finaux analysables | **Marts** |
| Le chef qui coordonne tout | Le programme qui lance chaque étape dans l'ordre | **Airflow** |
| Les caméras de la cuisine | La surveillance en temps réel | **Monitoring (Grafana)** |

Maintenant, déroulons le service du début à la fin.

---

## 3. Le déroulé, étape par étape

### Étape 1 — Aller au marché (Ingestion)

**Ce qui se passe :** un programme Python contacte le site TMDB et lui demande : « donne-moi tes films ». TMDB répond par paquets de 20 films à la fois (on appelle ça des **pages**). Le programme demande les pages une par une jusqu'à en avoir assez.

**Le détail malin :** si TMDB ne répond pas (réseau coupé, surcharge), le programme **réessaie tout seul** plusieurs fois avant d'abandonner. Comme un livreur qui re-sonne à la porte si personne ne répond.

> **API** = une façon standardisée de demander des données à un service web. Comme un menu : on commande un plat précis (« les films »), on reçoit ce plat.

### Étape 2 — Ranger au frigo (Data Lake)

**Ce qui se passe :** les films téléchargés sont rangés dans un grand espace de stockage local appelé **MinIO**, sous un format de fichier compact appelé **Parquet**.

**Le détail malin :** chaque livraison est rangée dans un dossier daté (`ingestion_date=2026-05-19`). Comme on étiquette les produits du frigo avec leur date — ça évite de tout mélanger et on sait toujours ce qui est frais.

> **Data Lake** = un grand entrepôt où on jette les données brutes, sans encore les organiser. « Lake » (lac) parce qu'on y déverse tout, en vrac.
>
> **Parquet** = un format de fichier optimisé pour les données en tableau. Plus compact et plus rapide à lire qu'un fichier Excel classique.

### Étape 3 — Laver et éplucher (Spark Staging)

**Ce qui se passe :** un outil appelé **Spark** prend les données brutes et fait le ménage :
- corrige les types (transformer « 2024 » texte en vraie date) ;
- supprime les doublons (un même film présent deux fois) ;
- enlève ou comble les valeurs manquantes.

**Pourquoi c'est important :** les données brutes sont toujours sales. Comme des légumes couverts de terre : il faut les laver avant de cuisiner.

> **Spark** = un moteur de traitement de données conçu pour gérer de **très gros volumes** en répartissant le travail. Ici on a 60 films, mais le même code marcherait avec des millions.

### Étape 4 — Assembler les ingrédients (Spark Curated)

**Ce qui se passe :** Spark **croise** les données entre elles. Par exemple : un film a un code de genre `28` ; on va chercher dans une autre liste que `28 = Action`, et on associe les deux. On ajoute aussi des infos calculées (la décennie de sortie, une catégorie de popularité…).

**Le résultat :** des données **enrichies**, où tout est lisible et relié.

> **Curated** (« organisé », « enrichi ») = la donnée propre et prête à l'emploi, comme un plat précuisiné qu'il ne reste qu'à dresser.

### Étape 5 — Monter en cuisine professionnelle (Snowflake)

**Ce qui se passe :** les données enrichies sont envoyées dans **Snowflake**, une base de données puissante hébergée dans le cloud. C'est là que vivront durablement les données pour être interrogées.

**Le détail malin :** la connexion à Snowflake se fait avec une **clé de sécurité** (comme une carte magnétique) plutôt qu'un simple mot de passe — c'est plus sûr et ça permet l'automatisation totale.

> **Base de données** = un classeur géant et intelligent, où on peut retrouver instantanément n'importe quelle info par une question (« combien de films d'action ? »).
>
> **Cloud** = des serveurs hébergés ailleurs (chez un prestataire), accessibles par internet. On n'a pas la machine chez soi.

### Étape 6 — Dresser les assiettes (DBT)

**Ce qui se passe :** un outil appelé **DBT** réorganise les données dans Snowflake en **tableaux finaux** bien structurés, selon des « recettes » écrites à l'avance. Il **teste** aussi automatiquement que tout est correct (pas de doublon, pas de trou, les liens entre tableaux sont cohérents).

**Le résultat :** des tableaux prêts à être analysés par n'importe quel outil (Excel, Power BI, etc.).

> **DBT** = un outil qui transforme les données déjà en base, en écrivant les transformations comme du **code réutilisable et testé**. Comme un livre de recettes versionné.

### Étape 7 — Le chef coordonne tout (Airflow)

**Ce qui se passe :** toutes les étapes précédentes (1 à 6) ne se lancent pas à la main. Un orchestrateur appelé **Airflow** les enchaîne **automatiquement, dans le bon ordre** : il ne lance l'étape 3 que si l'étape 2 a réussi, etc.

**L'image :** c'est le chef de cuisine qui crie « les entrées sont prêtes, on lance les plats ! ». Si une étape échoue, Airflow peut la **relancer** automatiquement.

> **Orchestrateur** = un chef d'orchestre pour programmes. Il décide quoi lancer, quand, et dans quel ordre.
>
> **DAG** = le « plan de bataille » d'Airflow : la liste des étapes et leurs dépendances (« fais A, puis B, puis C »). Concrètement ici : 8 étapes en file indienne.

### Étape 8 — Les caméras de surveillance (Monitoring)

**Ce qui se passe :** pendant que tout tourne, deux outils (**Prometheus** et **Grafana**) **mesurent et affichent** en temps réel ce qui se passe : combien d'étapes tournent, combien ont réussi, combien de temps elles prennent.

**L'image :** les écrans de contrôle d'une cuisine, qui montrent en direct l'état de chaque poste.

> **Monitoring** = surveiller la santé du système en continu, via des graphiques. Si quelque chose ralentit ou plante, on le voit tout de suite.

---

## 4. Le schéma global, en version simple

```
   TMDB          MinIO         Spark          Snowflake        DBT          Tableaux
  (le marché)   (le frigo)   (la prépa)    (la cuisine pro)  (le dressage)  (les plats)

   films  ───►  rangés  ───►  nettoyés ───►  stockés    ───►  mis en  ───►  prêts à
   bruts        au frigo      + enrichis     durablement     forme         analyser

        └──────────────── Airflow : le chef qui coordonne tout ────────────────┘
        └──────────────── Grafana : les caméras de surveillance ───────────────┘
```

---

## 5. Glossaire express

Tous les termes techniques du projet, en une phrase chacun :

| Terme | En clair |
|---|---|
| **Pipeline** | Une chaîne automatisée qui transforme des données brutes en données utiles. |
| **API** | Un menu pour commander des données à un service web. |
| **Ingestion** | L'action d'aller chercher et télécharger les données. |
| **Data Lake** | Un entrepôt qui stocke les données brutes en vrac. |
| **MinIO** | Le logiciel qui fait office de Data Lake ici (compatible avec le stockage Amazon S3). |
| **Parquet** | Un format de fichier compact et rapide pour les données en tableau. |
| **Spark** | Un moteur qui traite de gros volumes de données en répartissant le travail. |
| **Staging** | L'étape de nettoyage des données. |
| **Curated** | L'étape d'enrichissement / de croisement des données. |
| **Snowflake** | Une base de données puissante hébergée dans le cloud. |
| **Base de données** | Un classeur géant interrogeable instantanément. |
| **DBT** | Un outil qui transforme et teste les données déjà en base, comme un livre de recettes versionné. |
| **Marts** | Les tableaux finaux, prêts pour l'analyse. |
| **Star schema** | Une façon d'organiser les tableaux : un tableau central (les films) relié à des tableaux satellites (genres, pays, dates…). En forme d'étoile. |
| **Airflow** | L'orchestrateur : le chef qui lance chaque étape dans le bon ordre. |
| **DAG** | Le plan des étapes et de leurs dépendances. |
| **Monitoring** | La surveillance en temps réel via des graphiques. |
| **Prometheus / Grafana** | Les deux outils qui mesurent (Prometheus) et affichent (Grafana) la surveillance. |
| **Docker** | Une technologie qui met chaque programme dans une « boîte » isolée et portable, pour qu'il marche pareil partout. |
| **Container** | Une de ces « boîtes » isolées. |
| **CI (intégration continue)** | Un robot qui vérifie automatiquement que le code est correct à chaque modification. |

---

## 6. Pourquoi autant d'outils différents ?

Question légitime : pourquoi ne pas tout faire avec un seul programme ?

Parce que **chaque outil est le meilleur dans sa spécialité** — comme une cuisine où chaque poste a son spécialiste :

| Outil | Sa spécialité | L'équivalent cuisine |
|---|---|---|
| Python | Aller chercher les données, logique simple | Le commis qui va au marché |
| MinIO | Stocker beaucoup, pas cher | Le frigo |
| Spark | Traiter de gros volumes vite | Le robot multifonction |
| Snowflake | Répondre à des questions complexes en base | Le piano de cuisine professionnel |
| DBT | Organiser et tester les transformations | Le livre de recettes |
| Airflow | Coordonner sans erreur | Le chef |
| Grafana | Montrer ce qui se passe | Les écrans de contrôle |

Utiliser le bon outil au bon endroit, c'est exactement ce qu'on attend d'un ingénieur data. C'est aussi ce qui se fait **en entreprise**.

---

## 7. La logique du code, sans lire une ligne de code

Le projet est rangé en **dossiers**, un par étape. Voici à quoi sert chacun :

| Dossier | Ce qu'il contient | En clair |
|---|---|---|
| `ingestion/` | Le script qui télécharge les films | « Aller au marché » |
| `spark/` | Les scripts de nettoyage et d'enrichissement | « Laver + assembler » |
| `snowflake_load/` | Le script qui envoie les données dans Snowflake | « Monter en cuisine » |
| `dbt/` | Les recettes de mise en forme + les tests | « Dresser les assiettes » |
| `dags/` | Le plan d'orchestration d'Airflow | « Le carnet du chef » |
| `monitoring/` | La configuration de la surveillance | « Brancher les caméras » |

**Le principe de fond :** chaque étape lit ce que l'étape précédente a produit, fait son travail, et écrit le résultat pour l'étape suivante. Personne ne modifie le travail des autres — chaque poste reçoit, transforme, transmet.

C'est ce qui rend le tout **fiable** : si une étape plante, on sait exactement où, et on peut la relancer **sans tout recommencer**.

---

## 8. Les 3 niveaux de qualité (le secret de l'organisation)

Le projet range les données en **3 niveaux de propreté croissante**. C'est ce qu'on appelle l'**architecture medallion** (comme les médailles : bronze, argent, or).

| Niveau | Nom | État de la donnée | Analogie |
|---|---|---|---|
| 🥉 | **RAW** | Brute, telle que reçue, jamais modifiée | Les légumes pleins de terre |
| 🥈 | **STAGING** | Nettoyée, typée, dédupliquée | Les légumes lavés et coupés |
| 🥇 | **MARTS** | Mise en forme, enrichie, testée | Le plat dressé et servi |

**Pourquoi garder le brut (RAW) ?** Parce que si on se trompe dans le nettoyage, on peut **toujours repartir de l'original**. On ne jette jamais les ingrédients bruts tant que le plat n'est pas validé.

---

## 9. Questions naïves (mais légitimes)

**« Pourquoi des films ? »**
> Parce que TMDB offre des données riches, gratuites et bien structurées. C'est un terrain de jeu idéal. Le projet marcherait pareil avec des données de ventes, de météo, etc.

**« 60 films, c'est ridicule, non ? »**
> C'est volontaire, pour que la démo soit rapide. Le même code traiterait des millions de films en changeant **un seul réglage** (`TMDB_MAX_PAGES`). Ce qui compte, c'est la chaîne, pas le volume.

**« Tout ça pour ranger des données ? »**
> Oui, et c'est tout le métier de l'ingénieur data : rendre les données **fiables, propres et accessibles** pour ceux qui les analysent ensuite. Sans cette plomberie invisible, aucune analyse n'est fiable.

**« Que se passe-t-il si on relance deux fois ? »**
> Rien de cassé. Le système est conçu pour être **rejouable** : relancer pour la même journée écrase proprement, sans créer de doublons. Comme refaire une recette : on ne sert pas deux fois le même plat.

**« Et si internet coupe pendant le téléchargement ? »**
> Le programme **réessaie automatiquement**. Et si une étape échoue malgré tout, le chef (Airflow) peut la relancer sans toucher au reste.

**« C'est utilisé en vrai, ces outils ? »**
> Oui, tous. Airflow, Spark, Snowflake, DBT, Docker, Prometheus, Grafana sont des standards utilisés quotidiennement dans les entreprises tech. Le projet reproduit une vraie architecture professionnelle, en miniature.

---

## 10. Ce qu'il faut retenir

1. Le projet est une **chaîne automatisée** qui transforme des données brutes de films en tableaux propres et analysables.
2. Il fonctionne comme une **cuisine de restaurant** : du marché à l'assiette, chaque poste a son rôle.
3. Chaque étape est **isolée, fiable et rejouable** — si ça casse, on sait où et on relance.
4. Le tout est **coordonné** (Airflow), **surveillé** (Grafana) et **testé automatiquement** (CI + tests DBT).
5. Les outils utilisés sont ceux des **vraies entreprises** — le projet est une maquette grandeur réelle.

> Pour aller plus loin : [`README.md`](../README.md) (présentation), [`doc.md`](doc.md) (technique complète), [`key_command.md`](key_command.md) (mode d'emploi), [`notice_démo.md`](notice_démo.md) (déroulé de la démo).
